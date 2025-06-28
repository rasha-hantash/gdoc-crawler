package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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

// PatcherConfig holds the patcher configuration
type PatcherConfig struct {
	ProjectID        string
	Verbose          bool
	RateLimitDelay   time.Duration // Delay between API calls to stay under rate limits
	MaxRetryAttempts int           // Maximum retry attempts for failed requests
}

// DefaultPatcherConfig returns a default patcher configuration
func DefaultPatcherConfig() PatcherConfig {
	return PatcherConfig{
		Verbose:          true,
		RateLimitDelay:   1100 * time.Millisecond, // Stay ≤ 60 req/min
		MaxRetryAttempts: 6,
	}
}

// Patcher handles patching hyperlinks in uploaded Google Docs
type Patcher struct {
	docsService *docs.Service
	config      PatcherConfig

	// Pre-compiled regex for finding Google Docs/Sheets links
	linkRe *regexp.Regexp
}

// NewPatcher creates a new patcher with the given configuration
func NewPatcher(ctx context.Context, config PatcherConfig) (*Patcher, error) {
	opts := []option.ClientOption{}
	if config.ProjectID != "" {
		opts = append(opts, option.WithQuotaProject(config.ProjectID))
	}

	dsvc, err := docs.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating Docs service: %w", err)
	}

	return &Patcher{
		docsService: dsvc,
		config:      config,
		linkRe:      regexp.MustCompile(`https://docs\.google\.com/(document|spreadsheets)/d/([^/?#]+)`),
	}, nil
}

// PatchStats tracks patching statistics
type PatchStats struct {
	DocsProcessed int
	LinksPatched  int
	DocsSkipped   int
	Failures      int
}

// Run starts the patching process
func (p *Patcher) Run(ctx context.Context, outDir string, wait <-chan struct{}) error {
	<-wait // block until uploader signals completion

	idMap, err := p.loadIDMap(outDir)
	if err != nil {
		p.logf("no id_map.json found, skipping patching: %v", err)
		return nil
	}

	p.logf("patcher loaded %d ID mappings", len(idMap))

	stats := &PatchStats{}
	err = p.processAllDocs(ctx, outDir, idMap, stats)
	if err != nil {
		return fmt.Errorf("processing documents: %w", err)
	}

	p.logf("Patching completed: %d docs processed, %d links patched, %d skipped, %d failures",
		stats.DocsProcessed, stats.LinksPatched, stats.DocsSkipped, stats.Failures)

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
func (p *Patcher) processAllDocs(ctx context.Context, outDir string, idMap map[string]string, stats *PatchStats) error {
	return filepath.WalkDir(outDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() || d.Name() != "metadata.json" {
			return nil
		}

		if err := p.processDocument(ctx, path, idMap, stats); err != nil {
			p.logf("WARN processing document %s: %v", path, err)
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

	p.logf("patched %-40s (%d links)", metadata.Title, linksPatched)

	// Rate limiting to stay under API limits
	time.Sleep(p.config.RateLimitDelay)

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

	for i := 0; i < p.config.MaxRetryAttempts; i++ {
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

		p.logf("Retrying after 503 error (attempt %d/%d)", i+1, p.config.MaxRetryAttempts)
	}

	return fmt.Errorf("failed after %d attempts with 503 errors", p.config.MaxRetryAttempts)
}

// stripQuery removes query parameters and fragments from URLs
func (p *Patcher) stripQuery(url string) string {
	if i := strings.IndexAny(url, "?#"); i != -1 {
		return url[:i]
	}
	return url
}

// logf logs a message if verbose logging is enabled
func (p *Patcher) logf(format string, v ...any) {
	if p.config.Verbose {
		log.Printf(format, v...)
	}
}

// RunPatcher provides backward compatibility with the old API
func RunPatcher(outDir string, projectID string, wait <-chan struct{}) {
	ctx := context.Background()

	config := PatcherConfig{
		ProjectID:        projectID,
		Verbose:          true,
		RateLimitDelay:   1100 * time.Millisecond,
		MaxRetryAttempts: 6,
	}

	patcher, err := NewPatcher(ctx, config)
	if err != nil {
		logf("FATAL: failed to create patcher: %v", err)
		return
	}

	if err := patcher.Run(ctx, outDir, wait); err != nil {
		logf("FATAL: patcher failed: %v", err)
		return
	}

	log.Println("patcher done")
}

// Helper functions that are still needed globally
func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func mustOpen(p string) *os.File {
	f, err := os.Open(p)
	must(err)
	return f
}

func mustJSON(v any) []byte {
	b, err := json.MarshalIndent(v, "", "  ")
	must(err)
	return b
}

func canonicalLink(raw string) string {
	u := raw
	for i := 0; i < 3 && redirectRe.MatchString(u); i++ {
		parsed, _ := url.Parse(u)
		q := parsed.Query().Get("q")
		if q == "" {
			break
		}
		u, _ = url.QueryUnescape(q)
	}
	return u
}

// Legacy retry function for backward compatibility
func retry[T any](fn func() (T, error)) (T, error) {
	const maxAttempts = 6
	const base = time.Second
	var zero T
	for i := 0; i < maxAttempts; i++ {
		v, err := fn()
		if err == nil {
			return v, nil
		}
		if g, ok := err.(*googleapi.Error); !ok || g.Code != 503 {
			return zero, err
		}
		d := base * time.Duration(math.Pow(2, float64(i)))
		d += time.Duration(rand.Int63n(int64(d / 2)))
		time.Sleep(d)
	}
	return zero, fmt.Errorf("after %d attempts, still failing with 503", maxAttempts)
}

// Legacy helper functions
func stripQuery(u string) string {
	if i := strings.IndexAny(u, "?#"); i != -1 {
		return u[:i]
	}
	return u
}

func buildURLMap(htmlPath string, re *regexp.Regexp, idMap map[string]string) map[string]string {
	data, _ := os.ReadFile(htmlPath)
	matches := re.FindAllSubmatch(data, -1)
	urlMap := map[string]string{}
	for _, m := range matches {
		kind := string(m[1]) // document | spreadsheets
		oldID := string(m[2])
		oldKey := map[string]string{"document": "doc:" + oldID, "spreadsheets": "sheet:" + oldID}[kind]
		newID, ok := idMap[oldKey]
		if !ok {
			continue
		}
		oldURL := stripQuery(string(m[0]))
		newURL := fmt.Sprintf("https://docs.google.com/%s/d/%s/edit", kind, newID)
		urlMap[oldURL] = newURL
	}
	return urlMap
}

func patchOneDoc(ctx context.Context, dsvc *docs.Service, docID string, urlMap map[string]string) {
	doc, err := dsvc.Documents.Get(docID).Do()
	if err != nil {
		logf("patch fetch: %v", err)
		return
	}

	var reqs []*docs.Request
	for _, se := range doc.Body.Content {
		p := se.Paragraph
		if p == nil {
			continue
		}
		for _, pe := range p.Elements {
			tr := pe.TextRun
			if tr == nil || tr.TextStyle == nil || tr.TextStyle.Link == nil {
				continue
			}
			oldURL := canonicalLink(tr.TextStyle.Link.Url)
			newURL, ok := urlMap[oldURL]
			if !ok {
				continue
			}
			reqs = append(reqs, &docs.Request{
				UpdateTextStyle: &docs.UpdateTextStyleRequest{
					Range:     &docs.Range{StartIndex: pe.StartIndex, EndIndex: pe.EndIndex},
					TextStyle: &docs.TextStyle{Link: &docs.Link{Url: newURL}},
					Fields:    "link",
				},
			})
		}
	}
	if len(reqs) == 0 {
		return
	}

	_, err = retry(func() (*docs.BatchUpdateDocumentResponse, error) {
		return dsvc.Documents.BatchUpdate(docID, &docs.BatchUpdateDocumentRequest{Requests: reqs}).Do()
	})
	if err != nil {
		logf("patch update: %v", err)
	}
}
