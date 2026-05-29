package services

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/stjudewashere/seonaut/internal/config"
	"github.com/stjudewashere/seonaut/internal/crawler"
	"github.com/stjudewashere/seonaut/internal/models"
)

const (
	CrawlLimit      = 20000 // Max number of page reports that will be created
	LastCrawlsLimit = 5     // Max number returned by GetLastCrawls
	ClientTimeout   = 10    // HTTP client timeout in seconds.
)

// ErrProjectCrawling is returned when a crawl is requested for a project that
// is already being crawled. It is exported so callers (e.g. the API crawl
// service) can detect the condition with errors.Is instead of a string compare.
var ErrProjectCrawling = errors.New("project is already being crawled")

type CrawlerServiceRepository interface {
	SaveCrawl(models.Project) (*models.Crawl, error)
	GetLastCrawl(p *models.Project) models.Crawl
	GetLastCrawls(models.Project, int) []models.Crawl
	DeleteCrawlData(c *models.Crawl)

	CountIssuesByPriority(int64, int) int
	UpdateCrawl(*models.Crawl)
}

type CrawlerServicesContainer struct {
	Broker         *Broker
	ReportManager  *ReportManager
	CrawlerHandler *CrawlerHandler
	ArchiveService *ArchiveService
	Config         *config.CrawlerConfig
}

type CrawlerService struct {
	repository     CrawlerServiceRepository
	config         *config.CrawlerConfig
	broker         *Broker
	reportManager  *ReportManager
	crawlerHandler *CrawlerHandler
	ArchiveService *ArchiveService
	crawlers       map[int64]*crawler.Crawler
	lock           *sync.RWMutex
}

func NewCrawlerService(r CrawlerServiceRepository, s CrawlerServicesContainer) *CrawlerService {
	return &CrawlerService{
		repository:     r,
		broker:         s.Broker,
		config:         s.Config,
		reportManager:  s.ReportManager,
		crawlerHandler: s.CrawlerHandler,
		ArchiveService: s.ArchiveService,
		crawlers:       make(map[int64]*crawler.Crawler),
		lock:           &sync.RWMutex{},
	}
}

// reservedCrawl holds the state created when a crawl is reserved, ready to be
// executed. It bundles the parsed URL, the crawler, the saved crawl record and
// the previous crawl whose data is removed once the new crawl completes.
type reservedCrawl struct {
	url           *url.URL
	crawler       *crawler.Crawler
	crawl         *models.Crawl
	previousCrawl models.Crawl
}

// StartCrawler creates a new crawler and crawls the project's URL.
// It adds a new crawler for the project, it returns an error if there's one already
// running or if there's an error creating it.
// Finally the previous crawl's data is removed and the crawl is returned.
func (s *CrawlerService) StartCrawler(p models.Project, b models.BasicAuth) error {
	r, err := s.reserveCrawl(p, b, CrawlLimit)
	if err != nil {
		return err
	}

	go s.executeCrawl(r, p)

	return nil
}

// reserveCrawl acquires the in-memory crawler lock and creates the crawl record
// for the project. It returns the reserved crawl ready to be executed, or an
// error if the project is already being crawled or the crawl could not be saved.
func (s *CrawlerService) reserveCrawl(p models.Project, b models.BasicAuth, crawlLimit int) (*reservedCrawl, error) {
	u, err := url.Parse(p.URL)
	if err != nil {
		return nil, err
	}

	if u.Path == "" {
		u.Path = "/"
	}

	// Acquire the in-memory lock before any DB writes so that a rejected
	// duplicate trigger cannot leave an orphaned crawl record with a NULL
	// end timestamp.
	c, err := s.addCrawler(u, &p, &b, crawlLimit)
	if err != nil {
		return nil, err
	}

	previousCrawl := s.repository.GetLastCrawl(&p)

	crawl, err := s.repository.SaveCrawl(p)
	if err != nil {
		s.removeCrawler(&p)
		return nil, err
	}

	return &reservedCrawl{url: u, crawler: c, crawl: crawl, previousCrawl: previousCrawl}, nil
}

// executeCrawl runs a reserved crawl to completion. It blocks until the crawl
// is done, then creates the issues, updates the crawl record and publishes the
// completion message. The project is taken by value so the cleanup defers
// operate on a stable copy.
func (s *CrawlerService) executeCrawl(r *reservedCrawl, p models.Project) {
	defer s.removeCrawler(&p)
	defer s.repository.DeleteCrawlData(&r.previousCrawl)

	callback := s.crawlerHandler.responseCallback(r.crawl, &p, r.crawler)

	if p.Archive {
		archiver, err := s.ArchiveService.GetArchiveWriter(&p)
		if err != nil {
			log.Printf("Failed to create archive: %v", err)
		} else {
			defer archiver.Close()
			callback = s.crawlerHandler.archiveWrapper(callback, archiver)
		}
	}

	r.crawler.OnResponse(callback)

	log.Printf("Crawling %s...", p.URL)
	r.crawler.AddRequest(&crawler.RequestMessage{URL: r.url, Data: crawlerData{}})

	// Calling Start() initiates the website crawling process and
	// blocks execution until the crawling is complete.
	r.crawler.Start()

	r.crawl.RobotstxtExists = r.crawler.RobotstxtExists()
	r.crawl.SitemapExists = r.crawler.SitemapExists()
	r.crawl.SitemapIsBlocked = r.crawler.SitemapIsBlocked()
	r.crawl.End = time.Now()

	s.broker.Publish(fmt.Sprintf("crawl-%d", p.Id), &models.Message{Name: "IssuesInit"})
	s.reportManager.CreateMultipageIssues(r.crawl)

	r.crawl.IssuesEnd = time.Now()
	r.crawl.CriticalIssues = s.repository.CountIssuesByPriority(r.crawl.Id, Critical)
	r.crawl.AlertIssues = s.repository.CountIssuesByPriority(r.crawl.Id, Alert)
	r.crawl.WarningIssues = s.repository.CountIssuesByPriority(r.crawl.Id, Warning)
	r.crawl.TotalIssues = r.crawl.CriticalIssues + r.crawl.AlertIssues + r.crawl.WarningIssues

	s.repository.UpdateCrawl(r.crawl)
	s.broker.Publish(fmt.Sprintf("crawl-%d", p.Id), &models.Message{Name: "CrawlEnd", Data: r.crawl.TotalURLs})
	log.Printf("Crawled %d urls in %s", r.crawl.TotalURLs, p.URL)
}

// Get a slice with 'LastCrawlsLimit' number of the crawls
func (s *CrawlerService) GetLastCrawls(p models.Project) []models.Crawl {
	crawls := s.repository.GetLastCrawls(p, LastCrawlsLimit)

	for len(crawls) < LastCrawlsLimit {
		crawls = append(crawls, models.Crawl{Start: time.Now()})
	}

	return crawls
}

// StopCrawler stops a crawler. If the crawler does not exsit it will just return.
func (s *CrawlerService) StopCrawler(p models.Project) {
	s.lock.Lock()
	defer s.lock.Unlock()

	crawler, ok := s.crawlers[p.Id]
	if !ok {
		return
	}

	crawler.Stop()
}

// AddCrawler creates a new project crawler and adds it to the crawlers map. It returns the crawler
// on success otherwise it returns an error indicating the crawler already exists or there was an
// error creating it.
func (s *CrawlerService) addCrawler(u *url.URL, p *models.Project, b *models.BasicAuth, crawlLimit int) (*crawler.Crawler, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.crawlers[p.Id]; ok {
		return nil, ErrProjectCrawling
	}

	options := &crawler.Options{
		CrawlLimit:      crawlLimit,
		IgnoreRobotsTxt: p.IgnoreRobotsTxt,
		FollowNofollow:  p.FollowNofollow,
		IncludeNoindex:  p.IncludeNoindex,
		CrawlSitemap:    p.CrawlSitemap,
		AllowSubdomains: p.AllowSubdomains,
	}

	mainDomain := strings.TrimPrefix(u.Host, "www.")

	httpClient := &http.Client{
		Timeout: ClientTimeout * time.Second,
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Make sure the user agent is not empty
	if p.UserAgent == "" {
		p.UserAgent = s.config.Agent
	}

	client := crawler.NewBasicClient(&crawler.ClientOptions{
		UserAgent:        p.UserAgent,
		BasicAuthDomains: []string{mainDomain, "www." + mainDomain},
		AuthUser:         b.AuthUser,
		AuthPass:         b.AuthPass,
	}, httpClient)

	// Creates a new crawler with the crawler's response handler.
	s.crawlers[p.Id] = crawler.NewCrawler(u, options, client)

	return s.crawlers[p.Id], nil
}

// RemoveCrawler removes a project's crawler from the crawlers map.
func (s *CrawlerService) removeCrawler(p *models.Project) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.crawlers, p.Id)
}
