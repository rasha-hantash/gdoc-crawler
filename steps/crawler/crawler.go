package crawler

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/rasha-hantash/gdoc-pipeline/steps/types"
	"golang.org/x/net/html"
	"google.golang.org/api/docs/v1"
	"google.golang.org/api/sheets/v4"
)

// CrawlStats holds statistics about the crawling process
type CrawlStats struct {
	TotalDocs   int
	TotalSheets int
}

// Document type configuration
type docConfig struct {
	exportURLTemplate string
	filename          string
	canExtractLinks   bool
}

var docConfigs = map[string]docConfig{
	"doc": {
		exportURLTemplate: "https://docs.google.com/document/d/%s/export?format=html",
		filename:          "content.html",
		canExtractLinks:   true,
	},
	"sheet": {
		exportURLTemplate: "https://docs.google.com/spreadsheets/d/%s/export?format=csv",
		filename:          "content.csv",
		canExtractLinks:   false,
	},
}

// Global regex patterns
var (
	redirectRe   = regexp.MustCompile(`^https?://(www\.)?google\.com/url`)
	googleDocsRe = regexp.MustCompile(`docs\.google\.com/(document|spreadsheets)/d/([^/?#]+)`)
	nonAlphaNum  = regexp.MustCompile(`[^a-z0-9]+`)
	multiHyphen  = regexp.MustCompile(`-{2,}`)
	titleTrimRE  = regexp.MustCompile(`\s*-\s*Google (Docs?|Sheets?)\s*$`)
)

// Crawler handles the crawling process with configurable settings and dependencies
type Crawler struct {
	httpClient *http.Client
	MaxDepth   int
	startURL   string
	outDir     string

	// Cached Google API services (initialized lazily)
	docsSvc   *docs.Service
	sheetsSvc *sheets.Service
}

// NewCrawler creates a new crawler with the given configuration
func NewCrawler(maxDepth int, httpTimeout time.Duration, startURL, outDir string, docSvc *docs.Service, sheetSvc *sheets.Service) *Crawler {
	return &Crawler{
		httpClient: &http.Client{Timeout: httpTimeout},
		MaxDepth:   maxDepth,
		startURL:   startURL,
		outDir:     outDir,
		docsSvc:    docSvc,
		sheetsSvc:  sheetSvc,
	}
}

// Name implements the Step interface
func (c *Crawler) Name() string {
	return "crawler"
}

// Run implements the Step interface and starts the crawling process
func (c *Crawler) Run(ctx context.Context) error {
	// Clean and create output directory
	if err := os.RemoveAll(c.outDir); err != nil {
		return fmt.Errorf("failed to remove output directory: %w", err)
	}
	if err := os.MkdirAll(c.outDir, 0o755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	start := time.Now()
	stats := &CrawlStats{}

	pendingLinks := []types.Links{{Link: c.startURL, Depth: 0, Parent: c.outDir}}
	processedURLs := make(map[string]string)

	slog.Info("starting crawl",
		slog.String("start_url", c.startURL),
		slog.String("output_dir", c.outDir),
		slog.Int("max_depth", c.MaxDepth))

	for len(pendingLinks) > 0 {
		currentLink := c.popLink(&pendingLinks)

		if currentLink.Depth > c.MaxDepth {
			continue
		}

		if err := c.processUrl(ctx, currentLink, processedURLs, &pendingLinks); err != nil {
			slog.Warn("error processing url",
				slog.String("url", currentLink.Link),
				slog.Any("error", err))
			continue
		}
	}

	slog.Info("crawl completed",
		slog.Duration("duration", time.Since(start)),
		slog.Int("total_docs", stats.TotalDocs),
		slog.Int("total_sheets", stats.TotalSheets))
	return nil
}

// popLink removes and returns the first link from the queue (FIFO)
func (c *Crawler) popLink(pendingLinks *[]types.Links) types.Links {
	link := (*pendingLinks)[0]
	*pendingLinks = (*pendingLinks)[1:]
	return link
}

func (c *Crawler) processUrl(ctx context.Context, task types.Links, processedURLs map[string]string, pendingLinks *[]types.Links) error {
	canonical, cleanURL := c.CanonicalizeURL(task.Link)
	if canonical == "" {
		return nil // Not a Google Doc/Sheet, skip
	}

	// Check for URLs that have already been processed and redirect to a different URL
	if dir, duplicate := processedURLs[canonical]; duplicate {
		targetRel, _ := filepath.Rel(task.Parent, dir)
		// Determine underlying document type (doc or sheet) for redirect metadata
		parts := strings.SplitN(canonical, ":", 2)
		docType := "doc"
		if len(parts) > 0 {
			docType = parts[0]
		}

		c.writeMetadata(filepath.Join(task.Parent, filepath.Base(dir)+"-redirect"), types.Metadata{
			Title:      filepath.Base(dir),
			ID:         extractID(canonical),
			SourceURL:  task.Link,
			Depth:      task.Depth,
			Type:       docType,
			IsRedirect: true,
			RedirectTo: targetRel,
		})
		slog.Info("duplicate url",
			slog.String("url", canonical),
			slog.String("redirect_to", targetRel))
		return nil
	}

	// Process based on type
	if strings.HasPrefix(canonical, "doc:") || strings.HasPrefix(canonical, "sheet:") {
		docType := strings.SplitN(canonical, ":", 2)[0]
		links, dir, err := c.scrapeContent(ctx, task, docType, canonical, cleanURL)
		if err != nil {
			return err
		}
		processedURLs[canonical] = dir

		// Only docs extract links for further crawling
		if docType == "doc" {
			*pendingLinks = append(*pendingLinks, links...)
		}
		return nil
	}

	return nil
}

// CanonicalizeURL normalizes any Google Docs/Sheets link so the crawler sees each logical
// document exactly once. Links to the same file can look wildly different:
//   - Google's redirector (`https://www.google.com/url?q=...`)
//   - Trailing path modifiers (`/edit`, `/view`, `/preview` …)
//   - Tracking query-string parameters (`?usp=sharing`, `&pli=1` …)
//   - Fragment identifiers (`#heading=h.gjdgxs`)
//
// If we compared raw URLs we would store duplicates and re-crawl the same file many times.
// Instead we collapse every variant to a *canonical key* and a cleaned URL:
//
//	key   →  "doc:<ID>" | "sheet:<ID>"
//	clean →  absolute URL without redirector, params or fragments
//
// The key feeds the `processedURLs` map so duplicates become lightweight redirect entries
// and are skipped on subsequent visits. See crawler_test.go for concrete examples.
func (c *Crawler) CanonicalizeURL(rawURL string) (canonicalKey, cleanURL string) {
	// Step 1: If a URL is a redirect of a another URL then unwrap redirects (max 3 levels)
	cleanURL = rawURL
	for i := 0; i < 3 && redirectRe.MatchString(cleanURL); i++ {
		parsed, err := url.Parse(cleanURL)
		if err != nil {
			break
		}
		q := parsed.Query().Get("q")
		if q == "" {
			break
		}
		unescaped, err := url.QueryUnescape(q)
		if err != nil {
			break
		}
		cleanURL = unescaped
	}

	// Step 2: Extract type and ID in one pass
	matches := googleDocsRe.FindStringSubmatch(cleanURL)
	if len(matches) < 3 {
		return "", cleanURL // Not a Google Doc/Sheet
	}

	docType := matches[1] // "document" or "spreadsheets"
	docID := matches[2]

	// Step 3: Create canonical key
	switch docType {
	case "document":
		canonicalKey = "doc:" + docID
	case "spreadsheets":
		canonicalKey = "sheet:" + docID
	default:
		return "", cleanURL
	}

	return canonicalKey, cleanURL
}

// extractID extracts just the ID from a canonical key
func extractID(canonicalKey string) string {
	parts := strings.SplitN(canonicalKey, ":", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return ""
}

func (c *Crawler) scrapeContent(ctx context.Context, t types.Links, docType, canonical, cleanURL string) ([]types.Links, string, error) {
	id := extractID(canonical)
	if id == "" {
		return nil, "", fmt.Errorf("could not extract %s ID from canonical %s", docType, canonical)
	}

	config, exists := docConfigs[docType]
	if !exists {
		return nil, "", fmt.Errorf("unsupported document type: %s", docType)
	}

	// Build export URL and fetch content
	exportURL := fmt.Sprintf(config.exportURLTemplate, id)
	resp, err := c.httpGet(ctx, exportURL)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("reading content: %w", err)
	}

	// Extract title and links (if applicable)
	var title string
	var links []types.Links
	if docConfigs[docType].canExtractLinks {
		links, err = c.ExtractLinks(content, docType, cleanURL, t.Depth)
		if err != nil {
			return nil, "", err
		}
	}

	// Extract title based on document type
	switch docType {
	case "sheet":
		// For sheets, extract title from preview page (CSV doesn't contain title)
		title, err = c.fetchSheetTitle(ctx, id)
		if err != nil {
			return nil, "", err
		}
	case "doc":
		// Try to extract title from HTML content first
		title = c.extractTitleFromHTML(content)
		// If HTML extraction fails, try API as fallback
		if title == "" {
			title, err = c.fetchDocTitle(ctx, id)
			if err != nil {
				return nil, "", err
			}
		}
	}

	// Set default title if still empty
	if title == "" {
		title = "Untitled " + strings.Title(docType)
	}

	slug := c.makeSlug(title, id)
	dir := filepath.Join(t.Parent, slug)

	// Create directory and write content
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, "", fmt.Errorf("creating directory: %w", err)
	}

	if err := os.WriteFile(filepath.Join(dir, config.filename), content, 0o644); err != nil {
		return nil, "", fmt.Errorf("writing content: %w", err)
	}

	// Update links parent directory now that we know the final dir
	for i := range links {
		links[i].Parent = dir
	}

	// Write metadata
	c.writeMetadata(dir, types.Metadata{
		Title:     title,
		ID:        id,
		SourceURL: t.Link,
		Depth:     t.Depth,
		Type:      docType,
	})

	slog.Info("saved url",
		slog.String("url", t.Link),
		slog.String("type", strings.Title(docType)),
		slog.String("dir", dir))
	return links, dir, nil
}

func (c *Crawler) fetchDocTitle(ctx context.Context, docID string) (string, error) {
	// Extract title from HTML content instead of using API
	// This is a fallback method when API is not available
	return "", nil // Return empty string to trigger fallback
}

// extractTitleFromHTML extracts the document title from HTML content
func (c *Crawler) extractTitleFromHTML(content []byte) string {
	root, err := html.Parse(bytes.NewReader(content))
	if err != nil {
		return ""
	}

	var title string
	var dfs func(*html.Node)

	dfs = func(n *html.Node) {
		// Look for the first link that contains the document title
		if n.Type == html.ElementNode && n.Data == "a" {
			if title == "" { // Only get the first link as title
				for _, attr := range n.Attr {
					if attr.Key == "href" {
						// Check if this is a self-reference link (contains the same doc ID)
						if strings.Contains(attr.Val, "docs.google.com/document") {
							// Get the text content of this link
							var linkText strings.Builder
							for child := n.FirstChild; child != nil; child = child.NextSibling {
								if child.Type == html.TextNode {
									linkText.WriteString(child.Data)
								}
							}
							title = strings.TrimSpace(linkText.String())
							if title != "" {
								return
							}
						}
					}
				}
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			dfs(child)
		}
	}

	dfs(root)
	return title
}

func (c *Crawler) fetchSheetTitle(ctx context.Context, sheetID string) (string, error) {
	// Fetch the preview page to extract title from HTML
	previewURL := fmt.Sprintf("https://docs.google.com/spreadsheets/d/%s/preview", sheetID)
	resp, err := c.httpGet(ctx, previewURL)
	if err != nil {
		return "", fmt.Errorf("fetching sheet preview: %w", err)
	}
	defer resp.Body.Close()

	root, err := html.Parse(resp.Body)
	if err != nil {
		return "", fmt.Errorf("parsing sheet preview HTML: %w", err)
	}

	title := c.extractHTMLTitle(root)
	return title, nil
}

// extractHTMLTitle extracts the title from the HTML <title> tag
func (c *Crawler) extractHTMLTitle(root *html.Node) string {
	var title string
	var dfs func(*html.Node)

	dfs = func(n *html.Node) {
		if title != "" {
			return
		}
		if n.Type == html.ElementNode && n.Data == "title" && n.FirstChild != nil {
			title = n.FirstChild.Data
			return
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			dfs(child)
		}
	}

	dfs(root)
	title = titleTrimRE.ReplaceAllString(title, "")
	return strings.TrimSpace(title)
}

func (c *Crawler) writeMetadata(dir string, m types.Metadata) {
	m.CrawledAt = time.Now().UTC()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		slog.Warn("failed to create metadata directory",
			slog.String("dir", dir),
			slog.Any("error", err))
		return
	}

	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		slog.Warn("failed to marshal metadata",
			slog.String("dir", dir),
			slog.Any("error", err))
		return
	}

	if err := os.WriteFile(filepath.Join(dir, "metadata.json"), b, 0o644); err != nil {
		slog.Warn("failed to write metadata",
			slog.String("dir", dir),
			slog.Any("error", err))
	}
}

// -------------------- HTTP and utility methods ------------------

func (c *Crawler) httpGet(ctx context.Context, u string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("GET %s: %s", u, resp.Status)
	}
	return resp, nil
}

func (c *Crawler) makeSlug(title, id string) string {
	s := strings.ToLower(title)
	s = nonAlphaNum.ReplaceAllString(s, "-")
	s = multiHyphen.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if len(s) > 60 {
		s = s[:60]
	}
	if s == "" {
		sum := sha1.Sum([]byte(id))
		s = fmt.Sprintf("%x", sum[:6])
	}
	return fmt.Sprintf("%s-%s", s, id[:6])
}

func (c *Crawler) resolve(base, href string) string {
	u, err := url.Parse(href)
	if err != nil {
		return href
	}

	// If it's already absolute, return as-is
	if u.IsAbs() {
		return href
	}

	// Resolve relative URL
	b, err := url.Parse(base)
	if err != nil {
		return href
	}
	return b.ResolveReference(u).String()
}

func (c *Crawler) ExtractLinks(content []byte, docType, cleanURL string, depth int) ([]types.Links, error) {
	var links []types.Links

	// Only process HTML content for docs
	root, err := html.Parse(bytes.NewReader(content))
	if err != nil {
		return nil, fmt.Errorf("parsing HTML: %w", err)
	}

	var dfs func(*html.Node)

	dfs = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					resolvedURL := c.resolve(cleanURL, attr.Val)
					canonical, cleanURL := c.CanonicalizeURL(resolvedURL)
					if canonical != "" {
						links = append(links, types.Links{
							Link:   cleanURL,
							Depth:  depth,
							Parent: "",
						})
					}
				}
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			dfs(child)
		}
	}

	dfs(root)

	// For sheets and other types, return empty title (will use fallback)
	return links, nil
}
