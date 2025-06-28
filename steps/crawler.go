package steps

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"log"
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

// Global regex pattern needed by patcher.go
var redirectRe = regexp.MustCompile(`^https?://(www\.)?google\.com/url`)

// Global logf function for package-wide logging (used by patcher.go and uploader.go)
func logf(format string, v ...any) {
	log.Printf(format, v...)
}

// Config holds the crawler configuration
type Config struct {
	HTTPTimeout time.Duration
	MaxDepth    int
	Verbose     bool
}

// DefaultConfig returns a default configuration
func DefaultConfig() Config {
	return Config{
		HTTPTimeout: 10 * time.Second,
		MaxDepth:    3,
		Verbose:     true,
	}
}

// Crawler handles the crawling process with configurable dependencies
type Crawler struct {
	httpClient *http.Client
	config     Config

	// Compiled regex patterns
	docRe       *regexp.Regexp
	sheetRe     *regexp.Regexp
	redirectRe  *regexp.Regexp
	nonAlphaNum *regexp.Regexp
	multiHyphen *regexp.Regexp
	titleTrimRE *regexp.Regexp
}

// NewCrawler creates a new crawler with the given configuration
func NewCrawler(config Config) *Crawler {
	return &Crawler{
		httpClient: &http.Client{Timeout: config.HTTPTimeout},
		config:     config,

		// Pre-compile regex patterns for better performance
		docRe:       regexp.MustCompile(`docs\.google\.com/document/d/([^/?#]+)`),
		sheetRe:     regexp.MustCompile(`docs\.google\.com/spreadsheets/d/([^/?#]+)`),
		redirectRe:  regexp.MustCompile(`^https?://(www\.)?google\.com/url`),
		nonAlphaNum: regexp.MustCompile(`[^a-z0-9]+`),
		multiHyphen: regexp.MustCompile(`-{2,}`),
		titleTrimRE: regexp.MustCompile(`\s*-\s*Google (Docs?|Sheets?)\s*$`),
	}
}

// Run starts the crawling process
func (c *Crawler) Run(ctx context.Context, startURL string, outDir string, out chan<- string) {
	defer close(out)

	start := time.Now()
	stats := &CrawlStats{}

	queue := []types.Task{{Link: startURL, Depth: 0, Parent: outDir}}
	seen := make(map[string]string)

	for len(queue) > 0 {
		task := queue[0]
		queue = queue[1:]

		if task.Depth > c.config.MaxDepth {
			continue
		}

		canonical := c.canonicalKey(task.Link)
		if canonical == "" {
			continue
		}

		if dir, duplicate := seen[canonical]; duplicate {
			c.writeRedirectMetadata(task.Parent, canonical, task.Link, dir, task.Depth)
			c.logf("↩︎ duplicate %s → redirect metadata", canonical)
			continue
		}

		if err := c.processTask(ctx, task, seen, &queue, out, stats); err != nil {
			c.logf("WARN processing task %s: %v", task.Link, err)
		}
	}

	duration := time.Since(start).Round(time.Millisecond)
	c.logf("crawler finished in %s — %d docs, %d sheets", duration, stats.TotalDocs, stats.TotalSheets)
}

// CrawlStats tracks crawling statistics
type CrawlStats struct {
	TotalDocs   int
	TotalSheets int
}

// processTask handles a single crawling task
func (c *Crawler) processTask(ctx context.Context, task types.Task, seen map[string]string, queue *[]types.Task, out chan<- string, stats *CrawlStats) error {
	canonical := c.canonicalKey(task.Link)

	switch {
	case strings.HasPrefix(canonical, "doc:"):
		return c.processDocument(ctx, task, seen, queue, out, stats)
	case strings.HasPrefix(canonical, "sheet:"):
		return c.processSheet(ctx, task, seen, out, stats)
	default:
		return fmt.Errorf("unsupported task type for URL: %s", task.Link)
	}
}

// processDocument handles document crawling
func (c *Crawler) processDocument(ctx context.Context, task types.Task, seen map[string]string, queue *[]types.Task, out chan<- string, stats *CrawlStats) error {
	nested, dir, err := c.scrapeDoc(ctx, task)
	if err != nil {
		return fmt.Errorf("scraping document: %w", err)
	}

	canonical := c.canonicalKey(task.Link)
	seen[canonical] = dir
	stats.TotalDocs++
	out <- dir
	*queue = append(*queue, nested...)

	return nil
}

// processSheet handles spreadsheet crawling
func (c *Crawler) processSheet(ctx context.Context, task types.Task, seen map[string]string, out chan<- string, stats *CrawlStats) error {
	dir, err := c.scrapeSheet(ctx, task)
	if err != nil {
		return fmt.Errorf("scraping sheet: %w", err)
	}

	canonical := c.canonicalKey(task.Link)
	seen[canonical] = dir
	stats.TotalSheets++
	out <- dir

	return nil
}

func (c *Crawler) scrapeDoc(ctx context.Context, t types.Task) ([]types.Task, string, error) {
	id := c.docRe.FindStringSubmatch(t.Link)[1]
	exportURL := fmt.Sprintf("https://docs.google.com/document/d/%s/export?format=html", id)

	resp, err := c.httpGet(ctx, exportURL)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}

	root, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, "", err
	}

	title := c.firstHrefText(root)
	if title == "" {
		title = c.extractHTMLTitle(root)
	}

	dir := filepath.Join(t.Parent, c.makeSlug(title, id))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, "", fmt.Errorf("creating directory: %w", err)
	}

	if err := os.WriteFile(filepath.Join(dir, "content.html"), data, 0o644); err != nil {
		return nil, "", fmt.Errorf("writing content file: %w", err)
	}

	c.writeMetadata(dir, types.Metadata{
		Title:     title,
		ID:        id,
		SourceURL: t.Link,
		Depth:     t.Depth,
		Type:      "doc",
	})

	// Discover nested links
	nested := c.extractLinks(root, t)
	c.logf("    saved Doc → %s (%d nested links)", dir, len(nested))
	return nested, dir, nil
}

// extractLinks extracts nested Google Docs/Sheets links from HTML
func (c *Crawler) extractLinks(root *html.Node, parentTask types.Task) []types.Task {
	var nested []types.Task

	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					href := canonicalLink(c.resolve(parentTask.Link, attr.Val))
					if href != "" && (c.docRe.MatchString(href) || c.sheetRe.MatchString(href)) {
						nested = append(nested, types.Task{
							Link:   href,
							Depth:  parentTask.Depth + 1,
							Parent: filepath.Join(parentTask.Parent, c.makeSlug("", c.extractIDFromURL(href))),
						})
					}
					break
				}
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)

	return nested
}

// extractIDFromURL extracts the document/sheet ID from a Google URL
func (c *Crawler) extractIDFromURL(url string) string {
	if matches := c.docRe.FindStringSubmatch(url); len(matches) > 1 {
		return matches[1]
	}
	if matches := c.sheetRe.FindStringSubmatch(url); len(matches) > 1 {
		return matches[1]
	}
	return ""
}

func (c *Crawler) scrapeSheet(ctx context.Context, t types.Task) (string, error) {
	id := c.sheetRe.FindStringSubmatch(t.Link)[1]
	title, _ := c.fetchSheetTitle(ctx, id)

	dir := filepath.Join(t.Parent, c.makeSlug(title, id))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating directory: %w", err)
	}

	csvURL := fmt.Sprintf("https://docs.google.com/spreadsheets/d/%s/export?format=csv", id)
	resp, err := c.httpGet(ctx, csvURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	outFile := filepath.Join(dir, "content.csv")
	f, err := os.Create(outFile)
	if err != nil {
		return "", fmt.Errorf("creating output file: %w", err)
	}
	defer f.Close()

	if _, err = io.Copy(f, resp.Body); err != nil {
		return "", fmt.Errorf("writing CSV content: %w", err)
	}

	c.writeMetadata(dir, types.Metadata{
		Title:     title,
		ID:        id,
		SourceURL: t.Link,
		Depth:     t.Depth,
		Type:      "sheet",
	})
	c.logf("    saved Sheet → %s", dir)
	return dir, nil
}

func (c *Crawler) logf(format string, v ...any) {
	if c.config.Verbose {
		log.Printf(format, v...)
	}
}

func (c *Crawler) writeRedirectMetadata(parent, canonical, src, targetDir string, depth int) {
	targetRel, _ := filepath.Rel(parent, targetDir)
	meta := types.Metadata{
		Title:      filepath.Base(targetDir),
		ID:         c.canonicalID(canonical),
		SourceURL:  src,
		Depth:      depth,
		Type:       "redirect",
		RedirectTo: targetRel,
	}
	c.writeMetadata(filepath.Join(parent, filepath.Base(targetDir)+"-redirect"), meta)
}

func (c *Crawler) writeMetadata(dir string, m types.Metadata) {
	m.CrawledAt = time.Now().UTC()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		c.logf("WARN: failed to create metadata directory: %v", err)
		return
	}

	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		c.logf("WARN: failed to marshal metadata: %v", err)
		return
	}

	if err := os.WriteFile(filepath.Join(dir, "metadata.json"), b, 0o644); err != nil {
		c.logf("WARN: failed to write metadata: %v", err)
	}
}

func (c *Crawler) canonicalID(canonical string) string {
	return strings.SplitN(canonical, ":", 2)[1]
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
		return canonicalLink(href)
	}
	b, _ := url.Parse(base)
	return canonicalLink(b.ResolveReference(u).String())
}

func (c *Crawler) canonicalKey(link string) string {
	link = canonicalLink(link)
	if c.docRe.MatchString(link) {
		return "doc:" + c.docRe.FindStringSubmatch(link)[1]
	}
	if c.sheetRe.MatchString(link) {
		return "sheet:" + c.sheetRe.FindStringSubmatch(link)[1]
	}
	return ""
}

// RunCrawler provides backward compatibility with the old API
func RunCrawler(startURL string, outDir string, out chan<- string) {
	crawler := NewCrawler(DefaultConfig())
	ctx := context.Background()
	crawler.Run(ctx, startURL, outDir, out)
}
