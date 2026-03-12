package crawler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

type Crawler interface {
	ListenAndServe(ctx context.Context, address string) error
}

type CrawlRequest struct {
	URLs      []string `json:"urls"`
	Workers   int      `json:"workers"`
	TimeoutMS int      `json:"timeout_ms"`
}

type CrawlResponse struct {
	URL        string `json:"url"`
	StatusCode int    `json:"status_code,omitempty"`
	Error      string `json:"error,omitempty"`
}

var _ Crawler = (*crawlerImpl)(nil)

type memoEntry struct {
	resp      CrawlResponse
	expiresAt time.Time
}

type crawlerImpl struct {
	client *http.Client

	initOnce sync.Once
	sf       *singleflight.Group
	cacheMu  sync.RWMutex
	cache    map[string]memoEntry
}

type Wrap struct {
	item  string
	index int
}

type Response struct {
	item  CrawlResponse
	index int
}

const (
	shutdownTimeout = 10 * time.Second
	cacheTTL        = time.Second
)

func New() *crawlerImpl {
	return &crawlerImpl{
		client: &http.Client{
			//nolint:mnd // http.Transport defaults tuning
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   runtime.GOMAXPROCS(-1),
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: time.Second * 3,
				ResponseHeaderTimeout: time.Minute,
			},
		},
	}
}

func (c *crawlerImpl) ensureInit() {
	c.initOnce.Do(func() {
		c.sf = new(singleflight.Group)
		c.cache = make(map[string]memoEntry)
	})
}

func (c *crawlerImpl) cacheGet(url string) (CrawlResponse, bool) {
	c.ensureInit()
	now := time.Now()
	c.cacheMu.RLock()
	v, ok := c.cache[url]
	c.cacheMu.RUnlock()
	if !ok {
		return CrawlResponse{}, false
	}
	if now.After(v.expiresAt) {
		withLock(&c.cacheMu, func() {
			if v2, ok2 := c.cache[url]; ok2 && now.After(v2.expiresAt) {
				delete(c.cache, url)
			}
		})
		return CrawlResponse{}, false
	}
	return v.resp, true
}

func (c *crawlerImpl) cacheSet(url string, resp CrawlResponse) {
	c.ensureInit()

	withLock(&c.cacheMu, func() {
		c.cache[url] = memoEntry{
			resp:      resp,
			expiresAt: time.Now().Add(cacheTTL),
		}
	})
}

func (c *crawlerImpl) ListenAndServe(ctx context.Context, address string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/crawl", c.crawlHandler)
	errCh := make(chan error, 1)
	srv := &http.Server{
		Addr:    address,
		Handler: mux,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return <-errCh
	}
}

func userError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}

	var uerr *url.Error
	if errors.As(err, &uerr) {
		return uerr.Err.Error()
	}
	return err.Error()
}

// фукнция для получения кода ответа сервера по url
func (c *crawlerImpl) retrieveResponseCode(ctx context.Context, url string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func (c *crawlerImpl) checkOneCached(ctx context.Context, u string) CrawlResponse {
	c.ensureInit()
	normalized, err := normalizeURL(u)
	if err != nil {
		return CrawlResponse{
			URL:   u,
			Error: userError(err),
		}
	}

	if resp, ok := c.cacheGet(normalized); ok {
		resp.URL = u
		return resp
	}

	v, _, _ := c.sf.Do(normalized, func() (any, error) {
		if resp, ok := c.cacheGet(normalized); ok {
			return resp, nil
		}

		resp := c.checkOne(ctx, normalized)
		resp.URL = normalized
		c.cacheSet(normalized, resp)
		return resp, nil
	})

	resp := v.(CrawlResponse)
	resp.URL = u
	return resp
}

func (c *crawlerImpl) checkOne(ctx context.Context, url string) CrawlResponse {
	res := CrawlResponse{URL: url}

	code, err := c.retrieveResponseCode(ctx, url)
	if err != nil {
		res.Error = userError(err)
		return res
	}
	res.StatusCode = code
	return res
}

func Generate[T, R any](ctx context.Context, data []T, f func(index int, e T) R, size int) <-chan R {
	result := make(chan R, size)

	go func() {
		defer close(result)

		for i := 0; i < len(data); i++ {
			select {
			case result <- f(i, data[i]):
			case <-ctx.Done():
				return
			}
		}
	}()

	return result
}

func wrapElement(index int, url string) Wrap {
	return Wrap{url, index}
}

func normalizeURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}

	u.Scheme = strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	port := u.Port()

	if (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
		port = ""
	}
	if port == "" {
		u.Host = host
	} else {
		u.Host = net.JoinHostPort(host, port)
	}

	ep := u.EscapedPath()
	if ep == "" {
		ep = "/"
	}
	decodedPath, err := url.PathUnescape(ep)
	if err != nil {
		return "", err
	}
	u.Path = path.Clean(decodedPath)
	if u.Path == "." {
		u.Path = "/"
	}
	u.RawPath = ""

	u.RawQuery = u.Query().Encode()

	u.Fragment = ""

	return u.String(), nil
}

func (c *crawlerImpl) crawlHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req CrawlRequest
	wg := sync.WaitGroup{}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if len(req.URLs) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode([]CrawlResponse{})
		return
	}

	if req.Workers == math.MaxInt {
		http.Error(w, "infinite number of workers", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(req.TimeoutMS)*time.Millisecond)
	defer cancel()
	out := Generate[string, Wrap](ctx, req.URLs, wrapElement, 10)

	result := make([]CrawlResponse, len(req.URLs))
	var resMu sync.Mutex
	for worker := 0; worker < req.Workers; worker++ {
		wg.Go(func() {
			for {
				select {
				case u, ok := <-out:
					if !ok {
						return
					}
					resp := c.checkOneCached(ctx, u.item)
					withLock(&resMu, func() {
						result[u.index] = resp
					})
				case <-ctx.Done():
					return
				}
			}
		})
	}
	wg.Wait()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(result)
}

func withLock(mutex sync.Locker, action func()) {
	mutex.Lock()
	defer mutex.Unlock()

	action()
}
