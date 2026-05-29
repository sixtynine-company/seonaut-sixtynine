package services

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/stjudewashere/seonaut/internal/config"
	"github.com/stjudewashere/seonaut/internal/models"
)

// systemUserEmail is the email of the bootstrap user that owns every system
// project created through the internal crawl API.
const systemUserEmail = "system@seonaut.local"

// Errors returned by the APICrawlService.
var (
	// ErrUnknownDepth is returned when an unrecognized crawl depth is requested.
	ErrUnknownDepth = errors.New("api crawl: unknown depth")

	// ErrCrawlNotFound is returned when no crawl with the given id is tracked.
	ErrCrawlNotFound = errors.New("api crawl: crawl not found")

	// ErrCrawlNotFinished is returned when results are requested for a crawl that
	// has not finished yet.
	ErrCrawlNotFinished = errors.New("api crawl: crawl not finished")
)

// APICrawlServiceRepository describes the data-access methods the API crawl
// service depends on. The signatures match the concrete repository methods so
// the container can satisfy this interface by embedding the repositories.
type APICrawlServiceRepository interface {
	FindProjectByURL(url string, uid int) (*models.Project, error)
	GetCrawledPagesCount(crawlId int64) int
	GetLastCrawl(p *models.Project) models.Crawl
	FindUserByEmail(email string) (*models.User, error)
}

// apiCrawlState is the in-memory record tracking a single API-initiated crawl.
// Progress is read live from the database; this only holds the lifecycle status
// and any error message.
type apiCrawlState struct {
	projectId int64
	url       string
	cap       int
	status    string
	errMsg    string
	createdAt time.Time
}

// APICrawlService drives crawls on behalf of the internal JSON API. It owns a
// system user, resolves one system project per URL, enforces page-cap,
// per-crawl timeout and concurrency guardrails, and tracks crawl lifecycle in
// an in-memory state map.
type APICrawlService struct {
	crawler *CrawlerService
	project *ProjectService
	user    *UserService
	issue   *IssueService
	config  *config.APIConfig
	repo    APICrawlServiceRepository

	sem     chan struct{}
	stateMu sync.RWMutex
	state   map[int64]*apiCrawlState

	resolveMu sync.Mutex
	userOnce  sync.Once
	userErr   error
	sysUserID int
}

// NewAPICrawlService builds an APICrawlService. The config is copied into a
// local value and per-field defaults are applied for any zero/nil field so the
// service is always safe to use even with a sparsely configured [api] section.
func NewAPICrawlService(crawler *CrawlerService, project *ProjectService, user *UserService, issue *IssueService, cfg *config.APIConfig, repo APICrawlServiceRepository) *APICrawlService {
	// Copy the config so applied defaults never mutate the caller's value.
	local := config.APIConfig{}
	if cfg != nil {
		local = *cfg
	}

	if local.HomepageCap == 0 {
		local.HomepageCap = 1
	}
	if local.KeyCap == 0 {
		local.KeyCap = 10
	}
	if local.FullCap == 0 {
		local.FullCap = 200
	}
	if local.DefaultTimeoutSeconds == 0 {
		local.DefaultTimeoutSeconds = 90
	}
	if local.MaxTimeoutSeconds == 0 {
		local.MaxTimeoutSeconds = 300
	}
	if local.MaxConcurrentCrawls == 0 {
		local.MaxConcurrentCrawls = 3
	}

	return &APICrawlService{
		crawler: crawler,
		project: project,
		user:    user,
		issue:   issue,
		config:  &local,
		repo:    repo,
		sem:     make(chan struct{}, local.MaxConcurrentCrawls),
		state:   map[int64]*apiCrawlState{},
	}
}

// effectiveCap resolves the page cap for a crawl from the depth, with an
// optional maxPages override, clamped to [1, FullCap].
func (s *APICrawlService) effectiveCap(depth string, maxPages int) (int, error) {
	var base int
	switch depth {
	case "homepage":
		base = s.config.HomepageCap
	case "key":
		base = s.config.KeyCap
	case "full":
		base = s.config.FullCap
	default:
		return 0, ErrUnknownDepth
	}

	if maxPages > 0 {
		base = maxPages
	}
	if base < 1 {
		base = 1
	}
	if base > s.config.FullCap {
		base = s.config.FullCap
	}

	return base, nil
}

// effectiveTimeout resolves the per-crawl timeout from the optional override,
// falling back to the default, clamped to [1s, MaxTimeoutSeconds].
func (s *APICrawlService) effectiveTimeout(timeoutSeconds int) time.Duration {
	t := s.config.DefaultTimeoutSeconds
	if timeoutSeconds > 0 {
		t = timeoutSeconds
	}
	if t < 1 {
		t = 1
	}
	if t > s.config.MaxTimeoutSeconds {
		t = s.config.MaxTimeoutSeconds
	}

	return time.Duration(t) * time.Second
}

// ensureSystemUser lazily creates (once) the bootstrap system user and caches
// its id. The user is created with a random password; an already-existing user
// is treated as success. Any error is cached and returned on later calls.
func (s *APICrawlService) ensureSystemUser() error {
	s.userOnce.Do(func() {
		buf := make([]byte, 32)
		if _, err := rand.Read(buf); err != nil {
			s.userErr = fmt.Errorf("api crawl: generating system user password: %w", err)
			return
		}
		pw := hex.EncodeToString(buf)

		if _, err := s.user.SignUp(systemUserEmail, pw, "en", "light"); err != nil && !errors.Is(err, ErrUserExists) {
			s.userErr = fmt.Errorf("api crawl: creating system user: %w", err)
			return
		}

		u, err := s.repo.FindUserByEmail(systemUserEmail)
		if err != nil {
			s.userErr = fmt.Errorf("api crawl: finding system user: %w", err)
			return
		}

		s.sysUserID = u.Id
	})

	return s.userErr
}

// resolveProject normalizes the URL and returns the system project for it,
// creating one if it does not already exist. The whole body runs under
// resolveMu so concurrent requests for the same new URL do not race to create
// duplicate projects.
func (s *APICrawlService) resolveProject(rawURL string) (models.Project, error) {
	s.resolveMu.Lock()
	defer s.resolveMu.Unlock()

	u, err := url.Parse(rawURL)
	if err != nil {
		return models.Project{}, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return models.Project{}, ErrProtocolNotSupported
	}

	u.Host = strings.ToLower(u.Host)
	u.Fragment = ""
	if u.Path == "" {
		u.Path = "/"
	}
	norm := u.String()

	p, err := s.repo.FindProjectByURL(norm, s.sysUserID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return models.Project{}, err
		}

		// Project does not exist yet: create it. Scope booleans default to
		// false (no robots ignore, no sitemap, no subdomains).
		np := models.Project{URL: norm, UserAgent: s.crawler.config.Agent}
		if err := s.project.SaveProject(&np, s.sysUserID); err != nil {
			return models.Project{}, err
		}

		// SaveProject does not populate the new id, so re-find to get it.
		p, err = s.repo.FindProjectByURL(norm, s.sysUserID)
		if err != nil {
			return models.Project{}, err
		}
	}

	return *p, nil
}

// Enqueue resolves the system project for the URL, reserves a crawl and spawns
// a goroutine to run it. It returns the crawl id and lifecycle status. If the
// URL is already being crawled it dedups to the in-progress crawl. The crawl
// starts in the "queued" state and transitions to "running" once it acquires a
// concurrency slot.
func (s *APICrawlService) Enqueue(rawURL, depth string, maxPages, timeoutSeconds int) (int64, string, error) {
	s.sweep()

	if err := s.ensureSystemUser(); err != nil {
		return 0, "", err
	}

	cap, err := s.effectiveCap(depth, maxPages)
	if err != nil {
		return 0, "", err
	}

	timeout := s.effectiveTimeout(timeoutSeconds)

	project, err := s.resolveProject(rawURL)
	if err != nil {
		return 0, "", err
	}

	r, err := s.crawler.reserveCrawl(project, models.BasicAuth{}, cap)
	if err != nil {
		// If the project is already being crawled, dedup to the in-progress
		// crawl instead of failing.
		if errors.Is(err, ErrProjectCrawling) {
			last := s.repo.GetLastCrawl(&project)
			status := "running"
			s.stateMu.RLock()
			if st, ok := s.state[last.Id]; ok {
				status = st.status
			}
			s.stateMu.RUnlock()
			return last.Id, status, nil
		}
		return 0, "", err
	}

	id := r.crawl.Id
	s.stateMu.Lock()
	s.state[id] = &apiCrawlState{
		projectId: project.Id,
		url:       project.URL,
		cap:       cap,
		status:    "queued",
		createdAt: time.Now(),
	}
	s.stateMu.Unlock()

	go s.runCrawl(r, project, timeout)

	return id, "queued", nil
}

// runCrawl runs a reserved crawl to completion. It acquires a concurrency slot
// (blocking, and therefore staying "queued", while at capacity), arms a
// per-crawl timeout that stops the crawl on expiry (leaving partial results),
// and always releases the slot, even on panic.
func (s *APICrawlService) runCrawl(r *reservedCrawl, project models.Project, timeout time.Duration) {
	id := r.crawl.Id

	// Registered first so it runs last: after the slot is released and the
	// timer is stopped.
	defer func() {
		if rec := recover(); rec != nil {
			s.setStatus(id, "error", fmt.Sprint(rec))
		}
	}()

	s.sem <- struct{}{}        // blocks while at cap → stays "queued"
	defer func() { <-s.sem }() // always release, even on panic

	s.setStatus(id, "running", "")

	timer := time.AfterFunc(timeout, func() { s.crawler.StopCrawler(project) })
	defer timer.Stop()

	s.crawler.executeCrawl(r, project) // blocks until persisted

	s.setStatus(id, "done", "")
}

// CrawlProgress reports live crawl progress.
type CrawlProgress struct {
	Crawled int `json:"crawled"`
	Cap     int `json:"cap"`
	Percent int `json:"percent"`
}

// CrawlStatus is the status payload for a tracked crawl.
type CrawlStatus struct {
	Status   string        `json:"status"`
	Progress CrawlProgress `json:"progress"`
	Error    string        `json:"error,omitempty"`
}

// IssueItem is a single issue type with its count and affected URLs.
type IssueItem struct {
	Type  string   `json:"type"`
	Count int      `json:"count"`
	URLs  []string `json:"urls"`
}

// ResultTotals aggregates the crawl result counters.
type ResultTotals struct {
	Crawled     int `json:"crawled"`
	Critical    int `json:"critical"`
	High        int `json:"high"`
	Low         int `json:"low"`
	TotalIssues int `json:"totalIssues"`
}

// CrawlResult is the full results payload for a finished crawl.
type CrawlResult struct {
	CrawlId int64                  `json:"crawlId"`
	URL     string                 `json:"url"`
	Status  string                 `json:"status"`
	Totals  ResultTotals           `json:"totals"`
	Issues  map[string][]IssueItem `json:"issues"`
}

// Status returns the current status of a tracked crawl. The second return value
// is false when the crawl id is unknown. Progress is read live from the
// database: percent is 100 when done, otherwise capped at 99.
func (s *APICrawlService) Status(id int64) (CrawlStatus, bool) {
	s.stateMu.RLock()
	st, ok := s.state[id]
	if !ok {
		s.stateMu.RUnlock()
		return CrawlStatus{}, false
	}
	status := st.status
	errMsg := st.errMsg
	capValue := st.cap
	s.stateMu.RUnlock()

	crawled := s.repo.GetCrawledPagesCount(id)

	percent := 100
	if status != "done" {
		percent = min(99, crawled*100/max(capValue, 1))
	}

	return CrawlStatus{
		Status: status,
		Progress: CrawlProgress{
			Crawled: crawled,
			Cap:     capValue,
			Percent: percent,
		},
		Error: errMsg,
	}, true
}

// Results returns the full results for a finished crawl. It returns
// ErrCrawlNotFound for an unknown crawl id and ErrCrawlNotFinished (wrapped with
// the current status) when the crawl is not yet done.
func (s *APICrawlService) Results(id int64) (*CrawlResult, error) {
	s.stateMu.RLock()
	st, ok := s.state[id]
	if !ok {
		s.stateMu.RUnlock()
		return nil, ErrCrawlNotFound
	}
	status := st.status
	projectURL := st.url
	s.stateMu.RUnlock()

	if status != "done" {
		return nil, fmt.Errorf("%w: %s", ErrCrawlNotFinished, status)
	}

	ic := s.issue.GetIssuesCount(id)

	critical := s.buildIssueItems(id, ic.CriticalIssues)
	high := s.buildIssueItems(id, ic.AlertIssues)
	low := s.buildIssueItems(id, ic.WarningIssues)

	criticalCount := sumCounts(ic.CriticalIssues)
	highCount := sumCounts(ic.AlertIssues)
	lowCount := sumCounts(ic.WarningIssues)

	return &CrawlResult{
		CrawlId: id,
		URL:     projectURL,
		Status:  status,
		Totals: ResultTotals{
			Crawled:     s.repo.GetCrawledPagesCount(id),
			Critical:    criticalCount,
			High:        highCount,
			Low:         lowCount,
			TotalIssues: criticalCount + highCount + lowCount,
		},
		Issues: map[string][]IssueItem{
			"critical": critical,
			"high":     high,
			"low":      low,
		},
	}, nil
}

// buildIssueItems turns a slice of IssueGroups into IssueItems, fetching the
// first page of affected URLs for each issue type. On a pagination error the
// URLs are left empty (best effort).
func (s *APICrawlService) buildIssueItems(id int64, groups []models.IssueGroup) []IssueItem {
	items := make([]IssueItem, 0, len(groups))
	for _, g := range groups {
		urls := []string{}
		view, err := s.issue.GetPaginatedReportsByIssue(id, 1, g.ErrorType)
		if err == nil {
			for i := range view.PageReports {
				urls = append(urls, view.PageReports[i].URL)
			}
		}

		items = append(items, IssueItem{
			Type:  g.ErrorType,
			Count: g.Count,
			URLs:  urls,
		})
	}

	return items
}

// sumCounts returns the total Count across a slice of IssueGroups.
func sumCounts(groups []models.IssueGroup) int {
	total := 0
	for _, g := range groups {
		total += g.Count
	}

	return total
}

// setStatus updates the lifecycle status (and optional error message) of a
// tracked crawl. It is a no-op when the crawl id is not tracked.
func (s *APICrawlService) setStatus(id int64, status, errMsg string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if st, ok := s.state[id]; ok {
		st.status = status
		st.errMsg = errMsg
	}
}

// sweep removes finished (done/error) crawl state older than an hour. It is
// best-effort housekeeping to keep the state map from growing unbounded.
func (s *APICrawlService) sweep() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	for id, st := range s.state {
		if (st.status == "done" || st.status == "error") && time.Since(st.createdAt) > time.Hour {
			delete(s.state, id)
		}
	}
}
