package steps

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
)

// Global regex patterns
var (
	redirectRe   = regexp.MustCompile(`^https?://(www\.)?google\.com/url`)
	googleDocsRe = regexp.MustCompile(`docs\.google\.com/(document|spreadsheets)/d/([^/?#]+)`)
)

// canonicalizeURL performs all canonicalization in one pass:
// 1. Unwraps Google redirects
// 2. Extracts document type and ID
// 3. Returns canonical key ("doc:ID" or "sheet:ID") and clean URL
func canonicalizeURL(rawURL string) (canonicalKey, cleanURL string) {
	// Step 1: Unwrap redirects (max 3 levels)
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

// Config holds the crawler configuration
type Config struct {
	HTTPTimeout time.Duration
	MaxDepth    int
}

// DefaultConfig returns a default configuration
func DefaultConfig() Config {
	return Config{
		HTTPTimeout: 10 * time.Second,
		MaxDepth:    3,
	}
}

// Crawler handles the crawling process with configurable settings and dependencies
type Crawler struct {
	httpClient *http.Client
	config     Config

	// Step configuration
	startURL string
	outDir   string

	// Compiled regex patterns for text processing
	nonAlphaNum *regexp.Regexp
	multiHyphen *regexp.Regexp
	titleTrimRE *regexp.Regexp
}

// CrawlStats holds statistics about the crawling process
type CrawlStats struct {
	TotalDocs   int
	TotalSheets int
}

// NewCrawler creates a new crawler with the given configuration
func NewCrawler(config Config, startURL, outDir string) *Crawler {
	return &Crawler{
		httpClient: &http.Client{Timeout: config.HTTPTimeout},
		config:     config,
		startURL:   startURL,
		outDir:     outDir,

		// Pre-compile regex patterns for better performance
		nonAlphaNum: regexp.MustCompile(`[^a-z0-9]+`),
		multiHyphen: regexp.MustCompile(`-{2,}`),
		titleTrimRE: regexp.MustCompile(`\s*-\s*Google (Docs?|Sheets?)\s*$`),
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
		slog.Int("max_depth", c.config.MaxDepth))

	for len(pendingLinks) > 0 {
		currentLink := c.popLink(&pendingLinks)

		if currentLink.Depth > c.config.MaxDepth {
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
func (c *Crawler) popLink(queue *[]types.Links) types.Links {
	link := (*queue)[0]
	*queue = (*queue)[1:]
	return link
}

func (c *Crawler) processUrl(ctx context.Context, task types.Links, processedURLs map[string]string, queue *[]types.Links) error {
	canonical, _ := canonicalizeURL(task.Link)
	if canonical == "" {
		return nil // Not a Google Doc/Sheet, skip
	}

	// Check for URLs that have already been processed and redirect to a different URL
	if dir, duplicate := processedURLs[canonical]; duplicate {
		targetRel, _ := filepath.Rel(task.Parent, dir)
		c.writeMetadata(filepath.Join(task.Parent, filepath.Base(dir)+"-redirect"), types.Metadata{
			Title:      filepath.Base(dir),
			ID:         extractID(canonical),
			SourceURL:  task.Link,
			Depth:      task.Depth,
			Type:       "redirect",
			RedirectTo: targetRel,
		})
		slog.Info("duplicate url",
			slog.String("url", canonical),
			slog.String("redirect_to", targetRel))
		return nil
	}

	// Process based on type
	switch {
	case strings.HasPrefix(canonical, "doc:"):
		// Process document
		links, dir, err := c.scrapeContent(ctx, task, "doc")
		if err != nil {
			return err
		}
		processedURLs[canonical] = dir
		*queue = append(*queue, links...)
		return nil
	case strings.HasPrefix(canonical, "sheet:"):
		// Process sheet
		_, dir, err := c.scrapeContent(ctx, task, "sheet")
		if err != nil {
			return err
		}
		processedURLs[canonical] = dir
		return nil
	default:
		return nil
	}
}

func (c *Crawler) scrapeContent(ctx context.Context, t types.Links, docType string) ([]types.Links, string, error) {
	id := c.extractIDFromURL(t.Link)
	if id == "" {
		return nil, "", fmt.Errorf("could not extract %s ID from %s", docType, t.Link)
	}

	var title string
	var content []byte
	var filename string
	var exportURL string
	var links []types.Links

	switch docType {
	case "doc":
		exportURL = fmt.Sprintf("https://docs.google.com/document/d/%s/export?format=html", id)
		filename = "content.html"
	case "sheet":
		exportURL = fmt.Sprintf("https://docs.google.com/spreadsheets/d/%s/export?format=csv", id)
		filename = "content.csv"
	default:
		return nil, "", fmt.Errorf("unsupported document type: %s", docType)
	}

	// Fetch content
	resp, err := c.httpGet(ctx, exportURL)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	content, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("reading content: %w", err)
	}

	// Extract title based on type
	switch docType {
	case "doc":
		root, err := html.Parse(bytes.NewReader(content))
		if err != nil {
			return nil, "", fmt.Errorf("parsing HTML: %w", err)
		}
		title = c.extractHTMLTitle(root)
		if title == "" {
			title = c.firstHrefText(root)
		}
		if title == "" {
			title = "Untitled"
		}
		// Extract links for further crawling
		links = c.extractLinks(root, types.Links{Link: t.Link, Depth: t.Depth + 1, Parent: ""})
	case "sheet":
		// TODO Do i need to do it this way? why can't I fetch the title the same way as fetching a Google Doc
		title, err = c.fetchSheetTitle(ctx, id)
		if err != nil {
			slog.Warn("failed to get sheet title",
				slog.String("id", id),
				slog.Any("error", err))
			title = "Untitled Sheet"
		}
	}

	slug := c.makeSlug(title, id)
	dir := filepath.Join(t.Parent, slug)

	// Create directory and write content
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, "", fmt.Errorf("creating directory: %w", err)
	}

	if err := os.WriteFile(filepath.Join(dir, filename), content, 0o644); err != nil {
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

func (c *Crawler) extractLinks(root *html.Node, parentTask types.Links) []types.Links {
	var links []types.Links
	var dfs func(*html.Node)

	dfs = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					canonical, cleanURL := canonicalizeURL(c.resolve(parentTask.Link, attr.Val))
					if canonical != "" {
						links = append(links, types.Links{
							Link:   cleanURL,
							Depth:  parentTask.Depth,
							Parent: parentTask.Parent,
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
	return links
}

func (c *Crawler) extractIDFromURL(url string) string {
	canonical, _ := canonicalizeURL(url)
	return extractID(canonical)
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

func (c *Crawler) fetchSheetTitle(ctx context.Context, id string) (string, error) {
	u := fmt.Sprintf("https://docs.google.com/spreadsheets/d/%s/preview", id)
	resp, err := c.httpGet(ctx, u)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	root, err := html.Parse(resp.Body)
	if err != nil {
		return "", err
	}
	return c.extractHTMLTitle(root), nil
}

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
	title = c.titleTrimRE.ReplaceAllString(title, "")
	return strings.TrimSpace(title)
}

func (c *Crawler) firstHrefText(root *html.Node) string {
	var txt string
	var dfs func(*html.Node)
	dfs = func(n *html.Node) {
		if txt != "" {
			return
		}
		if n.Type == html.ElementNode && n.Data == "a" && n.FirstChild != nil {
			txt = c.nodeText(n)
			return
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			dfs(child)
		}
	}
	dfs(root)
	return strings.TrimSpace(txt)
}

func (c *Crawler) nodeText(n *html.Node) string {
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.TextNode {
			b.WriteString(n.Data)
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(n)
	return b.String()
}

func (c *Crawler) makeSlug(title, id string) string {
	s := strings.ToLower(title)
	s = c.nonAlphaNum.ReplaceAllString(s, "-")
	s = c.multiHyphen.ReplaceAllString(s, "-")
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
	if err != nil || u.IsAbs() {
		_, cleanURL := canonicalizeURL(href)
		return cleanURL
	}
	b, _ := url.Parse(base)
	_, cleanURL := canonicalizeURL(b.ResolveReference(u).String())
	return cleanURL
}

// RunCrawler provides backward compatibility with the old API
func RunCrawler(startURL string, outDir string, out chan<- string) {
	crawler := NewCrawler(DefaultConfig(), startURL, outDir)
	ctx := context.Background()
	if err := crawler.Run(ctx); err != nil {
		slog.Error("crawler failed", slog.Any("error", err))
	}
	close(out)
}
