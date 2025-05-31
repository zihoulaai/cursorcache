package test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/cache/v9"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/zihoulaai/cursorcache"
	"github.com/zihoulaai/cursorcache/redisbloom"
)

type Item struct {
	ID       int64     `gorm:"column:id;primaryKey" json:"id"`
	CreateAt time.Time `gorm:"column:create_at" json:"create_at"`
	Name     string    `gorm:"column:name" json:"name"`
}

type PageResult[T any, C any] struct {
	Items      []T `json:"items"`
	PrevCursor C   `json:"prev_cursor,omitempty"` // 只在有上一页时存在
	NextCursor C   `json:"next_cursor,omitempty"` // 只在有下一页时存在
}

// TestSuite 包含所有测试共享的资源
type TestSuite struct {
	suite.Suite
	ctx        context.Context
	db         *gorm.DB
	rdb        *redis.Client
	bloom      *redisbloom.RedisBloom
	cleanupDB  func()
	cleanupRDB func()
}

// 启动 Redis 容器并返回客户端
func (s *TestSuite) setupRedis() {
	containerReq := testcontainers.ContainerRequest{
		Image:        "redis/redis-stack-server:latest",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForListeningPort("6379/tcp"),
	}
	container, err := testcontainers.GenericContainer(s.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerReq,
		Started:          true,
	})
	require.NoError(s.T(), err)

	endpoint, err := container.Endpoint(s.ctx, "")
	require.NoError(s.T(), err)

	s.rdb = redis.NewClient(&redis.Options{Addr: endpoint})

	s.cleanupRDB = func() {
		_ = s.rdb.Close()
		_ = container.Terminate(s.ctx)
	}
}

// 启动 MySQL 容器并返回客户端
func (s *TestSuite) setupMySQL() {
	containerReq := testcontainers.ContainerRequest{
		Image:        "mysql:8.0",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "root",
			"MYSQL_DATABASE":      "test_db",
			"MYSQL_USER":          "test_user",
			"MYSQL_PASSWORD":      "test_pass",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("ready for connections"),
			wait.ForListeningPort("3306/tcp"),
		),
	}
	container, err := testcontainers.GenericContainer(s.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerReq,
		Started:          true,
	})
	require.NoError(s.T(), err)

	endpoint, err := container.Endpoint(s.ctx, "")
	require.NoError(s.T(), err)

	dsn := fmt.Sprintf("test_user:test_pass@tcp(%s)/test_db?charset=utf8mb4&parseTime=True&loc=Local", endpoint)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	require.NoError(s.T(), err, "failed to connect to MySQL")

	s.db = db
	s.cleanupDB = func() {
		sqlDB, err := db.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
		_ = container.Terminate(s.ctx)
	}
}

// 初始化测试数据
func (s *TestSuite) initData() {
	require.NoError(s.T(), s.db.AutoMigrate(&Item{}), "failed to migrate database")

	// 创建更多样化的测试数据
	baseTime := time.Unix(1748561153, 0)
	items := make([]Item, 0, 20)
	for i := 1; i <= 20; i++ {
		// 创建时间间隔，模拟真实场景
		createAt := baseTime.Add(time.Duration(i-1) * time.Minute)
		items = append(items, Item{
			ID:       int64(i),
			CreateAt: createAt,
			Name:     fmt.Sprintf("Item %d", i),
		})
	}

	// 批量插入提高效率
	require.NoError(s.T(), s.db.Create(&items).Error, "failed to insert items")
}

// 在每个测试前设置环境
func (s *TestSuite) SetupTest() {
	s.ctx = context.Background()
	s.setupRedis()
	s.setupMySQL()
	s.initData()

	// 初始化布隆过滤器
	s.bloom = redisbloom.NewRedisBloom(s.rdb, "test:bloom")
	require.NoError(s.T(), s.bloom.Init(s.ctx, 1000, 0.001), "bloom init failed")
}

// 在每个测试后清理资源
func (s *TestSuite) TearDownTest() {
	if s.cleanupDB != nil {
		s.cleanupDB()
	}
	if s.cleanupRDB != nil {
		s.cleanupRDB()
	}
}

// 分页查询实现
func paginateItems(db *gorm.DB, cursor *cursorcache.ComplexCursor, direction string, pageSize int) (*PageResult[Item, *cursorcache.ComplexCursor], error) {
	var items []Item
	query := db.Model(&Item{}).Order("create_at DESC, id DESC")

	if cursor != nil {
		switch direction {
		case "next": // 请求下一页（更旧的数据）
			query = query.Where(
				"(create_at < ?) OR (create_at = ? AND id < ?)",
				cursor.CreatedAt, cursor.CreatedAt, cursor.ID,
			)
		case "prev": // 请求上一页（更新的数据）
			query = query.Where(
				"(create_at > ?) OR (create_at = ? AND id > ?)",
				cursor.CreatedAt, cursor.CreatedAt, cursor.ID,
			)
		default:
			return nil, fmt.Errorf("invalid direction: %s", direction)
		}
	}

	// 查询当前页
	if err := query.Limit(pageSize).Find(&items).Error; err != nil {
		return nil, err
	}

	if len(items) == 0 {
		return &PageResult[Item, *cursorcache.ComplexCursor]{Items: items}, nil
	}

	result := &PageResult[Item, *cursorcache.ComplexCursor]{Items: items}
	firstItem := items[0]
	lastItem := items[len(items)-1]

	// 1. 检查是否有上一页（比当前页第一条记录更新的数据）
	var prevCount int64
	err := db.Model(&Item{}).Where(
		"(create_at > ?) OR (create_at = ? AND id > ?)",
		firstItem.CreateAt, firstItem.CreateAt, firstItem.ID,
	).Limit(1).Count(&prevCount).Error

	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	if prevCount > 0 {
		result.PrevCursor = &cursorcache.ComplexCursor{
			CreatedAt: firstItem.CreateAt,
			ID:        firstItem.ID,
		}
	}

	// 2. 检查是否有下一页（比当前页最后一条记录更旧的数据）
	var nextCount int64
	err = db.Model(&Item{}).Where(
		"(create_at < ?) OR (create_at = ? AND id < ?)",
		lastItem.CreateAt, lastItem.CreateAt, lastItem.ID,
	).Limit(1).Count(&nextCount).Error

	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	if nextCount > 0 {
		result.NextCursor = &cursorcache.ComplexCursor{
			CreatedAt: lastItem.CreateAt,
			ID:        lastItem.ID,
		}
	}

	return result, nil
}

// 创建数据获取函数
func mockDB(db *gorm.DB) cursorcache.FetcherFunc[Item] {
	return func(ctx context.Context, cursorStr string, size int, dir cursorcache.CursorDirection) ([]Item, string, string, error) {
		var cursor *cursorcache.ComplexCursor
		if cursorStr != "" {
			var err error
			cursor, err = cursorcache.DecodeCursor(cursorStr)
			if err != nil {
				return nil, "", "", fmt.Errorf("decode cursor: %w", err)
			}
		}

		page, err := paginateItems(db, cursor, string(dir), size)
		if err != nil {
			return nil, "", "", fmt.Errorf("paginate items: %w", err)
		}

		return page.Items,
			cursorcache.EncodeCursor(page.NextCursor),
			cursorcache.EncodeCursor(page.PrevCursor), nil
	}
}

// 测试分页逻辑
func (s *TestSuite) TestPaginationLogic() {
	t := s.T()
	fetcher := mockDB(s.db)

	// 测试第一页
	items, next, prev, err := fetcher(s.ctx, "", 5, cursorcache.NextPage)
	require.NoError(t, err)
	require.Len(t, items, 5)
	require.NotEmpty(t, next, "next cursor should exist")
	require.Empty(t, prev, "prev cursor should not exist on first page")

	// 验证第一页数据顺序 (最新在前)
	for i := 0; i < 4; i++ {
		assert.True(t, items[i].CreateAt.After(items[i+1].CreateAt) ||
			(items[i].CreateAt.Equal(items[i+1].CreateAt) && items[i].ID > items[i+1].ID),
			"items should be in descending order")
	}

	// 测试下一页
	items2, next2, prev2, err := fetcher(s.ctx, next, 5, cursorcache.NextPage)
	require.NoError(t, err)
	require.Len(t, items2, 5)
	require.NotEmpty(t, next2, "next cursor should exist")
	require.NotEmpty(t, prev2, "prev cursor should exist")

	// 验证第一页最后一条和第二页第一条的连续性
	lastOfFirst := items[len(items)-1]
	firstOfSecond := items2[0]
	assert.True(t, firstOfSecond.CreateAt.Before(lastOfFirst.CreateAt) ||
		(firstOfSecond.CreateAt.Equal(lastOfFirst.CreateAt) && firstOfSecond.ID < lastOfFirst.ID),
		"items should be contiguous")

	// 测试上一页 (应返回第一页)
	itemsPrev, nextPrev, prevPrev, err := fetcher(s.ctx, prev2, 5, cursorcache.PrevPage)
	require.NoError(t, err)
	require.Len(t, itemsPrev, 5)
	require.Equal(t, items[0].ID, itemsPrev[0].ID, "should return to first page")
	require.Equal(t, next, nextPrev, "next cursor should match original")
	require.Empty(t, prevPrev, "prev cursor should be empty on first page")

	// 前进到最后一页
	var lastNext string
	for {
		pageItems, n, _, err := fetcher(s.ctx, next, 5, cursorcache.NextPage)
		require.NoError(t, err)

		if n == "" {
			// 到达最后一页
			require.LessOrEqual(t, len(pageItems), 5, "last page should have <= 5 items")
			require.Greater(t, len(pageItems), 0, "last page should have items")
			break
		}
		next = n
		lastNext = n
	}

	// 测试最后一页没有下一页游标
	lastPage, nextLast, prevLast, err := fetcher(s.ctx, lastNext, 5, cursorcache.NextPage)
	require.NoError(t, err)
	require.Empty(t, nextLast, "next cursor should be empty on last page")
	require.NotEmpty(t, prevLast, "prev cursor should exist on last page")
	require.Len(t, lastPage, 5, "last page should have full page")
}

// 测试缓存功能
func (s *TestSuite) TestCacheFunctionality() {
	t := s.T()
	fetcher := mockDB(s.db)

	page := cursorcache.NewCursorCache[Item](cache.New(&cache.Options{
		Redis: s.rdb,
		//LocalCache: cache.NewTinyLFU(100, 0.1),
		Marshal:      json.Marshal,
		Unmarshal:    json.Unmarshal,
		StatsEnabled: false,
	}),
		cursorcache.WithBloomFilter[Item](s.bloom),
		cursorcache.WithTTL[Item](10*time.Second),
		cursorcache.WithRandomRange[Item](1),
	)

	req := cursorcache.PageRequest[Item]{
		Cursor:    "",
		Direction: cursorcache.NextPage,
		PageSize:  5,
		KeyPrefix: "test_page",
		Fetcher:   fetcher,
	}

	// 第一次请求 - 应缓存未命中
	resp, err := page.Paginate(s.ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.Data, 5)
	require.False(t, resp.FromCache, "first request should not be from cache")
	require.NotEmpty(t, resp.NextCursor, "next cursor should exist")

	// 第二次相同请求 - 应缓存命中
	resp2, err := page.Paginate(s.ctx, req)
	require.NoError(t, err)
	require.Len(t, resp2.Data, 5)
	require.True(t, resp2.FromCache, "second request should be from cache")

	// 测试缓存过期
	time.Sleep(10 * time.Second)
	resp3, err := page.Paginate(s.ctx, req)
	require.NoError(t, err)
	require.Len(t, resp3.Data, 5)
	require.False(t, resp3.FromCache, "request after TTL should not be from cache")

	// 测试不同游标的缓存
	req.Cursor = resp.NextCursor
	resp4, err := page.Paginate(s.ctx, req)
	require.NoError(t, err)
	require.Len(t, resp4.Data, 5)
	require.False(t, resp4.FromCache, "new cursor request should not be from cache")
}

// 测试并发请求
func (s *TestSuite) TestConcurrentRequests() {
	t := s.T()
	fetcher := mockDB(s.db)

	page := cursorcache.NewCursorCache[Item](cache.New(&cache.Options{
		Redis: s.rdb,
		//LocalCache: cache.NewTinyLFU(100, 0.1),
		Marshal:      json.Marshal,
		Unmarshal:    json.Unmarshal,
		StatsEnabled: false,
	}),
		cursorcache.WithBloomFilter[Item](s.bloom),
		cursorcache.WithTTL[Item](10*time.Second),
		cursorcache.WithRandomRange[Item](1),
	)

	req := cursorcache.PageRequest[Item]{
		Cursor:    "",
		Direction: cursorcache.NextPage,
		PageSize:  5,
		KeyPrefix: "concurrent_page",
		Fetcher:   fetcher,
	}

	const numRequests = 20
	results := make([]*cursorcache.PageResponse[Item], numRequests)
	errCh := make(chan error, numRequests)
	var wg sync.WaitGroup
	wg.Add(numRequests)

	// 记录缓存命中和未命中
	var cacheHits, cacheMisses int
	var mu sync.Mutex

	for i := 0; i < numRequests; i++ {
		go func(idx int) {
			defer wg.Done()
			resp, err := page.Paginate(s.ctx, req)
			if err != nil {
				errCh <- err
				return
			}

			results[idx] = resp

			mu.Lock()
			if resp.FromCache {
				cacheHits++
			} else {
				cacheMisses++
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	close(errCh)

	// 检查错误
	for err := range errCh {
		require.NoError(t, err, "pagination error")
	}

	// 验证所有结果一致
	firstResult := results[0]
	for i, resp := range results {
		if i == 0 {
			continue
		}
		require.Len(t, resp.Data, 5, "all responses should have 5 items")
		require.Equal(t, firstResult.Data[0].ID, resp.Data[0].ID, "all responses should have same data")
	}

	// 验证缓存行为
	require.Equal(t, 1, cacheMisses, "should have only one cache miss")
	require.Equal(t, numRequests-1, cacheHits, "should have cache hits for other requests")
}

// 运行测试套件
func TestCursorCacheSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}
