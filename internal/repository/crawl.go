package repository

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/stjudewashere/seonaut/internal/models"
)

type CrawlRepository struct {
	DB *sql.DB
}

// SaveCrawl inserts a new crawl into the database and returns a new Crawl model with
// the data provided by the project.
func (ds *CrawlRepository) SaveCrawl(p models.Project) (*models.Crawl, error) {
	ctx, cancel := writeCtx()
	defer cancel()
	stmt, err := ds.DB.PrepareContext(ctx, "INSERT INTO crawls (project_id) VALUES (?)")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, p.Id)

	if err != nil {
		return nil, err
	}

	cid, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}

	return &models.Crawl{
		Id:        cid,
		ProjectId: p.Id,
		URL:       p.URL,
		Start:     time.Now(),
	}, nil
}

// GetCrawledPagesCount returns the number of page reports stored for the given
// crawl. It is used to report live crawl progress.
func (ds *CrawlRepository) GetCrawledPagesCount(crawlId int64) int {
	ctx, cancel := readCtx()
	defer cancel()
	row := ds.DB.QueryRowContext(ctx, `SELECT count(*) FROM pagereports WHERE crawl_id = ?`, crawlId)
	var c int
	if err := row.Scan(&c); err != nil {
		log.Printf("GetCrawledPagesCount: %v\n", err)
	}
	return c
}

// GetLastCrawl returns a Crawl model with the last crawl stored for an specific project.
func (ds *CrawlRepository) GetLastCrawl(p *models.Project) models.Crawl {
	query := `
		SELECT
			id,
			start,
			end,
			total_urls,
			total_issues,
			critical_issues,
			alert_issues,
			warning_issues,
			issues_end,
			robotstxt_exists,
			sitemap_exists,
			sitemap_blocked,
			links_internal_follow,
			links_internal_nofollow,
			links_external_follow,
			links_external_nofollow,
			links_sponsored,
			links_ugc
		FROM crawls
		WHERE project_id = ?
		ORDER BY start DESC LIMIT 1`

	ctx, cancel := readCtx()
	defer cancel()
	row := ds.DB.QueryRowContext(ctx, query, p.Id)

	var endTime, issuesEndTime sql.NullTime
	crawl := models.Crawl{Crawling: true}
	err := row.Scan(
		&crawl.Id,
		&crawl.Start,
		&endTime, // &crawl.End,
		&crawl.TotalURLs,
		&crawl.TotalIssues,
		&crawl.CriticalIssues,
		&crawl.AlertIssues,
		&crawl.WarningIssues,
		&issuesEndTime, // &crawl.IssuesEnd,
		&crawl.RobotstxtExists,
		&crawl.SitemapExists,
		&crawl.SitemapIsBlocked,
		&crawl.InternalFollowLinks,
		&crawl.InternalNoFollowLinks,
		&crawl.ExternalFollowLinks,
		&crawl.ExternalNoFollowLinks,
		&crawl.SponsoredLinks,
		&crawl.UGCLinks,
	)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("GetLastCrawl project id %d: %v\n", p.Id, err)
	}

	if endTime.Valid && issuesEndTime.Valid {
		crawl.End = endTime.Time
		crawl.IssuesEnd = issuesEndTime.Time
		crawl.Crawling = false
	}

	return crawl
}

// GetLastCrawls returns a slice with a number of crawls for the specific project. The number of crawls
// to be returned is specified with the limit parameter.
func (ds *CrawlRepository) GetLastCrawls(p models.Project, limit int) []models.Crawl {
	query := `
		SELECT
			id,
			start,
			end,
			total_urls,
			total_issues,
			issues_end,
			critical_issues,
			alert_issues,
			warning_issues,
			blocked_by_robotstxt,
			noindex
		FROM crawls
		WHERE project_id = ?
		ORDER BY start DESC LIMIT ?`

	crawls := []models.Crawl{}
	ctx, cancel := readCtx()
	defer cancel()
	rows, err := ds.DB.QueryContext(ctx, query, p.Id, limit)
	if err != nil {
		log.Println(err)
		return crawls
	}
	defer rows.Close()

	for rows.Next() {
		endTime := sql.NullTime{}
		issuesEndTime := sql.NullTime{}
		crawl := models.Crawl{Crawling: true}
		err := rows.Scan(
			&crawl.Id,
			&crawl.Start,
			&endTime, // &crawl.End,
			&crawl.TotalURLs,
			&crawl.TotalIssues,
			&issuesEndTime, // &crawl.IssuesEnd,
			&crawl.CriticalIssues,
			&crawl.AlertIssues,
			&crawl.WarningIssues,
			&crawl.BlockedByRobotstxt,
			&crawl.Noindex,
		)
		if err != nil {
			log.Printf("GetLastCrawl: %v\n", err)
		}
		if endTime.Valid && issuesEndTime.Valid {
			crawl.End = endTime.Time
			crawl.IssuesEnd = issuesEndTime.Time
			crawl.Crawling = false
		}
		crawls = append([]models.Crawl{crawl}, crawls...)
	}

	return crawls
}

// DeleteCrawlData deletes all the crawl's data in a throttled batch process. It removes
// the crawl's associated links, external_links, hreflangs, issues, images and any other
// data associated to it. It is meant to run off the crawl's critical path (in a
// background goroutine): each DB call is context-bounded, a short pause between batches
// keeps it gentle on the database, and a per-table batch cap guarantees it can never
// loop unbounded even if a delete stops making progress.
func (ds *CrawlRepository) DeleteCrawlData(crawl *models.Crawl) {
	// 1000 rows per batch; the cap is a runaway backstop, far above any real crawl.
	const maxBatchesPerTable = 100000

	deleteTable := func(cid int64, table string) {
		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE crawl_id = ? ORDER BY id DESC LIMIT 1000", table)
		countQuery := fmt.Sprintf("SELECT count(*) FROM %s WHERE crawl_id = ?", table)

		for i := 0; i < maxBatchesPerTable; i++ {
			writeContext, writeCancel := writeCtx()
			_, err := ds.DB.ExecContext(writeContext, deleteQuery, cid)
			writeCancel()
			if err != nil {
				log.Printf("DeleteCrawlData: cid %d table %s %v\n", cid, table, err)
				return
			}

			readContext, readCancel := readCtx()
			row := ds.DB.QueryRowContext(readContext, countQuery, cid)
			var c int
			err = row.Scan(&c)
			readCancel()
			if err != nil {
				log.Printf("DeleteCrawlData count: cid %d table %s %v\n", cid, table, err)
				return
			}

			if c == 0 {
				return
			}

			// Brief pause between batches to stay gentle on the database.
			time.Sleep(250 * time.Millisecond)
		}

		log.Printf("DeleteCrawlData: cid %d table %s hit batch cap, leaving remainder for next cleanup\n", cid, table)
	}

	for _, table := range []string{
		"links", "external_links", "hreflangs", "issues", "images",
		"scripts", "styles", "iframes", "audios", "videos", "pagereports",
	} {
		deleteTable(crawl.Id, table)
	}
}

// DeleteProjectCrawls deletes all of the project's crawls and associated data.
func (ds *CrawlRepository) DeleteProjectCrawls(p *models.Project) {
	query := `
		SELECT
			id
		FROM crawls
		WHERE project_id = ?
	`

	ctx, cancel := readCtx()
	defer cancel()
	rows, err := ds.DB.QueryContext(ctx, query, p.Id)
	if err != nil {
		log.Printf("DeleteProjectCrawls Query: %v\n", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		c := &models.Crawl{}
		if err := rows.Scan(&c.Id); err != nil {
			log.Printf("DeleteProjectCrawls: %v\n", err)
		}

		ds.DeleteCrawlData(c)
	}

	query = `DELETE FROM crawls WHERE project_id = ?`
	writeContext, writeCancel := writeCtx()
	defer writeCancel()
	_, err = ds.DB.ExecContext(writeContext, query, p.Id)
	if err != nil {
		log.Printf("deleting crawls for project %d: %v", p.Id, err)
		return
	}
}

// Deletes all crawls that are unfinished and have the issues_end field set to null.
// It cleans up the crawl data for each unfinished crawl before deleting it.
func (ds *CrawlRepository) DeleteUnfinishedCrawls() {
	query := `
		SELECT
			crawls.id
		FROM crawls
		WHERE crawls.issues_end IS NULL
	`
	count := 0

	ctx, cancel := readCtx()
	defer cancel()
	rows, err := ds.DB.QueryContext(ctx, query)
	if err != nil {
		log.Println(err)
		return
	}
	defer rows.Close()

	ids := []any{}
	placeholders := []string{}
	for rows.Next() {
		c := &models.Crawl{}
		err := rows.Scan(&c.Id)
		if err != nil {
			log.Printf("DeleteUnfinishedCrawls: %v\n", err)
			continue
		}

		count++
		ds.DeleteCrawlData(c)
		ids = append(ids, c.Id)
		placeholders = append(placeholders, "?")
	}

	if len(ids) == 0 {
		return
	}

	placeholdersStr := strings.Join(placeholders, ",")
	deleteQuery := fmt.Sprintf("DELETE FROM crawls WHERE id IN (%s)", placeholdersStr)
	writeContext, writeCancel := writeCtx()
	defer writeCancel()
	_, err = ds.DB.ExecContext(writeContext, deleteQuery, ids...)
	if err != nil {
		log.Printf("DeleteUnfinishedCrawls: %v", err)
	}

	log.Printf("Deleted %d unfinished crawls.", count)
}

// SaveIssuesCount stores the total number of issues as well as the total issues by priority for
// the crawl specified in the "crawlId" parameter.
func (ds *CrawlRepository) UpdateCrawl(crawl *models.Crawl) {
	query := `UPDATE
		crawls
		SET 
			end = ?,
			total_urls = ?,
			blocked_by_robotstxt = ?,
			noindex = ?,
			robotstxt_exists = ?,
			sitemap_exists = ?,
			sitemap_blocked = ?,
			links_internal_follow = ?,
			links_internal_nofollow = ?,
			links_external_follow = ?,
			links_external_nofollow = ?,
			links_sponsored = ?,
			links_ugc = ?,
			issues_end = ?,
			critical_issues = ?,
			alert_issues = ?,
			warning_issues = ?,
			total_issues = ?
		WHERE id = ?`

	ctx, cancel := writeCtx()
	defer cancel()
	_, err := ds.DB.ExecContext(
		ctx,
		query,
		crawl.End,
		crawl.TotalURLs,
		crawl.BlockedByRobotstxt,
		crawl.Noindex,
		crawl.RobotstxtExists,
		crawl.SitemapExists,
		crawl.SitemapIsBlocked,
		crawl.InternalFollowLinks,
		crawl.InternalNoFollowLinks,
		crawl.ExternalFollowLinks,
		crawl.ExternalNoFollowLinks,
		crawl.SponsoredLinks,
		crawl.UGCLinks,
		crawl.IssuesEnd,
		crawl.CriticalIssues,
		crawl.AlertIssues,
		crawl.WarningIssues,
		crawl.TotalIssues,
		crawl.Id,
	)
	if err != nil {
		log.Printf("SaveIssuesCount: %v\n", err)
	}
}
