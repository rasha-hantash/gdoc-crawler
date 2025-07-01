package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/rasha-hantash/gdoc-pipeline/steps/types"
	"google.golang.org/api/docs/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// Patcher handles patching hyperlinks in uploaded Google Docs
type Patcher struct {
	docsService      *docs.Service
	rateLimitDelay   time.Duration
	maxRetryAttempts int

	// Step configuration
	outDir string

	// Pre-compiled regex for finding Google Docs/Sheets links
	linkRe *regexp.Regexp
}

// NewPatcher creates a new patcher with the given configuration
func NewPatcher(ctx context.Context, projectID string, rateLimitDelay time.Duration, maxRetryAttempts int, outDir string) (*Patcher, error) {
	opts := []option.ClientOption{}
	if projectID != "" {
		opts = append(opts, option.WithQuotaProject(projectID))
	}

	dsvc, err := docs.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating Docs service: %w", err)
	}

	return &Patcher{
		docsService:      dsvc,
		rateLimitDelay:   rateLimitDelay,
		maxRetryAttempts: maxRetryAttempts,
		outDir:           outDir,
		linkRe:           regexp.MustCompile(`https://docs\.google\.com/(document|spreadsheets)/d/([^/?#]+)`),
	}, nil
}

// PatchStats tracks patching statistics
type PatchStats struct {
	DocsProcessed int
	LinksPatched  int
	DocsSkipped   int
	Failures      int
}

// Name implements the Step interface
func (p *Patcher) Name() string {
	return "patcher"
}

// Run implements the Step interface and starts the patching process
func (p *Patcher) Run(ctx context.Context) error {
	idMap, err := p.loadIDMap(p.outDir)
	if err != nil {
		slog.Info("no id_map.json found, skipping patching", slog.Any("error", err))
		return nil
	}

	slog.Info("patcher started", slog.Int("id_mappings", len(idMap)))

	stats := &PatchStats{}
	err = p.processAllDocs(ctx, idMap, stats)
	if err != nil {
		return fmt.Errorf("processing documents: %w", err)
	}

	slog.Info("patching completed",
		slog.Int("docs_processed", stats.DocsProcessed),
		slog.Int("links_patched", stats.LinksPatched),
		slog.Int("docs_skipped", stats.DocsSkipped),
		slog.Int("failures", stats.Failures))

	return nil
}

// loadIDMap loads the ID mapping from the output directory
func (p *Patcher) loadIDMap(outDir string) (map[string]string, error) {
	mapPath := filepath.Join(outDir, "id_map.json")
	f, err := os.Open(mapPath)
	if err != nil {
		return nil, fmt.Errorf("opening id_map.json: %w", err)
	}
	defer f.Close()

	var idMap map[string]string
	if err := json.NewDecoder(f).Decode(&idMap); err != nil {
		return nil, fmt.Errorf("decoding id_map.json: %w", err)
	}

	return idMap, nil
}

// processAllDocs walks through all directories and patches documents
func (p *Patcher) processAllDocs(ctx context.Context, idMap map[string]string, stats *PatchStats) error {
	return filepath.WalkDir(p.outDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() || d.Name() != "metadata.json" {
			return nil
		}

		if err := p.processDocument(ctx, path, idMap, stats); err != nil {
			slog.Warn("processing document failed",
				slog.String("path", path),
				slog.Any("error", err))
			stats.Failures++
		}

		return nil
	})
}

// processDocument processes a single document for link patching
func (p *Patcher) processDocument(ctx context.Context, metaPath string, idMap map[string]string, stats *PatchStats) error {
	metadata, err := p.loadDocumentMetadata(metaPath)
	if err != nil {
		return fmt.Errorf("loading metadata: %w", err)
	}

	if metadata.IsRedirect {
		stats.DocsSkipped++
		return nil // Skip redirects
	}

	if metadata.Type != "doc" {
		stats.DocsSkipped++
		return nil // Only patch documents, not sheets
	}

	newDocID := idMap["doc:"+metadata.ID]
	if newDocID == "" {
		stats.DocsSkipped++
		return nil // No uploaded version found
	}

	dir := filepath.Dir(metaPath)
	htmlPath := filepath.Join(dir, "content.html")

	urlMap, err := p.buildURLMap(htmlPath, idMap)
	if err != nil {
		return fmt.Errorf("building URL map: %w", err)
	}

	if len(urlMap) == 0 {
		stats.DocsProcessed++
		return nil // No links to patch
	}

	linksPatched, err := p.patchDocumentLinks(ctx, newDocID, urlMap)
	if err != nil {
		return fmt.Errorf("patching document links: %w", err)
	}

	stats.DocsProcessed++
	stats.LinksPatched += linksPatched

	slog.Info("patched document",
		slog.String("title", metadata.Title),
		slog.Int("links_patched", linksPatched))

	// Rate limiting to stay under API limits
	time.Sleep(p.rateLimitDelay)

	return nil
}

// loadDocumentMetadata loads metadata from a metadata.json file
func (p *Patcher) loadDocumentMetadata(metaPath string) (*types.Metadata, error) {
	f, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var metadata types.Metadata
	if err := json.NewDecoder(f).Decode(&metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// buildURLMap builds a mapping of old URLs to new URLs based on the ID map
func (p *Patcher) buildURLMap(htmlPath string, idMap map[string]string) (map[string]string, error) {
	data, err := os.ReadFile(htmlPath)
	if err != nil {
		return nil, fmt.Errorf("reading HTML file: %w", err)
	}

	matches := p.linkRe.FindAllSubmatch(data, -1)
	urlMap := make(map[string]string)

	for _, match := range matches {
		kind := string(match[1]) // document | spreadsheets
		oldID := string(match[2])

		// Map document type to our internal key format
		typeMap := map[string]string{
			"document":     "doc:" + oldID,
			"spreadsheets": "sheet:" + oldID,
		}

		oldKey := typeMap[kind]
		newID, exists := idMap[oldKey]
		if !exists {
			continue // Skip if no mapping found
		}

		oldURL := p.stripQuery(string(match[0]))
		newURL := fmt.Sprintf("https://docs.google.com/%s/d/%s/edit", kind, newID)
		urlMap[oldURL] = newURL
	}

	return urlMap, nil
}

// patchDocumentLinks patches all links in a single document
func (p *Patcher) patchDocumentLinks(ctx context.Context, docID string, urlMap map[string]string) (int, error) {
	doc, err := p.docsService.Documents.Get(docID).Do()
	if err != nil {
		return 0, fmt.Errorf("fetching document: %w", err)
	}

	requests := p.buildPatchRequests(doc, urlMap)
	if len(requests) == 0 {
		return 0, nil // No links to patch
	}

	err = p.executeWithRetry(ctx, func() error {
		_, err := p.docsService.Documents.BatchUpdate(docID, &docs.BatchUpdateDocumentRequest{
			Requests: requests,
		}).Do()
		return err
	})

	if err != nil {
		return 0, fmt.Errorf("executing batch update: %w", err)
	}

	return len(requests), nil
}

// buildPatchRequests builds a list of patch requests for document links
func (p *Patcher) buildPatchRequests(doc *docs.Document, urlMap map[string]string) []*docs.Request {
	var requests []*docs.Request

	for _, structuralElement := range doc.Body.Content {
		paragraph := structuralElement.Paragraph
		if paragraph == nil {
			continue
		}

		for _, element := range paragraph.Elements {
			textRun := element.TextRun
			if textRun == nil || textRun.TextStyle == nil || textRun.TextStyle.Link == nil {
				continue
			}

			// TODO: this needs to remove the /edit from the URL
			oldURL := canonicalLink(textRun.TextStyle.Link.Url)
			newURL, exists := urlMap[oldURL]
			if !exists {
				continue
			}

			requests = append(requests, &docs.Request{
				UpdateTextStyle: &docs.UpdateTextStyleRequest{
					Range: &docs.Range{
						StartIndex: element.StartIndex,
						EndIndex:   element.EndIndex,
					},
					TextStyle: &docs.TextStyle{
						Link: &docs.Link{Url: newURL},
					},
					Fields: "link",
				},
			})
		}
	}

	return requests
}

// executeWithRetry executes a function with exponential backoff retry logic
func (p *Patcher) executeWithRetry(ctx context.Context, fn func() error) error {
	const base = time.Second

	for i := 0; i < p.maxRetryAttempts; i++ {
		err := fn()
		if err == nil {
			return nil
		}

		// Only retry on 503 backend errors
		if googleAPIErr, ok := err.(*googleapi.Error); !ok || googleAPIErr.Code != 503 {
			return err
		}

		// Calculate exponential backoff with jitter
		delay := base * time.Duration(math.Pow(2, float64(i)))
		jitter := time.Duration(rand.Int63n(int64(delay / 2)))
		time.Sleep(delay + jitter)

		slog.Info("retrying after 503 error",
			slog.Int("attempt", i+1),
			slog.Int("max_attempts", p.maxRetryAttempts))
	}

	return fmt.Errorf("failed after %d attempts with 503 errors", p.maxRetryAttempts)
}

// stripQuery removes query parameters and fragments from URLs
func (p *Patcher) stripQuery(url string) string {
	if i := strings.IndexAny(url, "?#"); i != -1 {
		return url[:i]
	}
	return url
}

// pre-compiled once; matches "doc … /d/<ID>" or "spreadsheets … /d/<ID>"
var tidyRE = regexp.MustCompile(`^(https://docs\.google\.com/(?:document|spreadsheets)/d/[^/]+)`)

// canonicalLink unwraps Google's redirector and drops tracking params.
func canonicalLink(raw string) string {
	u := raw

	// ── 1. unwrap Google redirector ──────────────────────────────────────────
	for i := 0; i < 3 && strings.Contains(u, "://www.google.com/url?"); i++ {
		parsed, _ := url.Parse(u)
		real := parsed.Query().Get("q")
		if real == "" {
			break
		}
		real, _ = url.QueryUnescape(real)
		u = real
	}

	// ── 2. strip ?query and #fragment ────────────────────────────────────────
	if i := strings.IndexAny(u, "?#"); i != -1 {
		u = u[:i]
	}

	// ── 3. drop trailing /edit, /view, /preview … ───────────────────────────
	if m := tidyRE.FindStringSubmatch(u); len(m) > 0 {
		u = m[1] // keep only the part through "…/d/<ID>"
	}

	return u
}
