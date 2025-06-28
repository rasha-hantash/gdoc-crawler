// gdoc_pipeline.go — concurrent crawler + Drive uploader + hyperlink patcher
//
// Three goroutines coordinate via two channels:
//   • crawler   — walks a public Google Doc/Sheet graph, writes each page to
//                 disk and sends its directory path over the paths channel.
//   • uploader  — receives directory paths, uploads the file (HTML/CSV) to
//                 Drive, builds id_map.json, then closes doneUpload.
//   • patcher   — waits for doneUpload (so id_map.json is available), then
//                 scans every crawled Doc and uses the Docs API to rewrite
//                 internal hyperlinks to point at the newly‑uploaded copies.
//
// Usage (all flags are optional aside from -url):
//   go run gdoc_pipeline.go \
//        -url     "https://docs.google.com/document/d/XXXXXXXX/edit" \
//        -out     ./out \
//        -depth   8 \
//        -httptimeout 15s \
//        -folder  "Imported Docs" \
//        -project "my-gcp-project" \
//        -v
//
// Notes
// -----
// * The uploader throttles Drive API automatically via the Docs client‑side
//   library; patcher adds explicit back‑off for 503 errors.
// * For clarity this is still a single file. In production, consider splitting
//   into packages (crawler, driveutil, patcher) and adding context cancelation.

package main

import (
	"context"
	"flag"
	"log"
	"os"
	"sync"
	"time"

	"github.com/rasha-hantash/gdoc-pipeline/steps"
)

// ---------------------------------------------------------------------------
// CLI flags (union of the three original tools)
// ---------------------------------------------------------------------------
var (
	// Crawler
	startURL    string
	outDir      string
	maxDepth    int
	httpTimeout time.Duration

	// Uploader / Patcher
	driveFolder string
	projectID   string

	// Misc
	verbose bool
)

func init() {
	flag.StringVar(&startURL, "url", "", "Public Google Doc/Sheet URL to start crawling from")
	flag.StringVar(&outDir, "out", "./out", "Output directory")
	flag.IntVar(&maxDepth, "depth", 5, "Maximum depth for nested Docs/Sheets")
	flag.DurationVar(&httpTimeout, "httptimeout", 15*time.Second, "HTTP timeout per request")

	flag.StringVar(&driveFolder, "folder", "Imported Docs", "Drive folder (created if absent)")
	flag.StringVar(&projectID, "project", "", "GCP quota-project (optional)")

	flag.BoolVar(&verbose, "v", false, "Verbose logging")
	flag.BoolVar(&verbose, "verbose", false, "Verbose logging (alias of -v)")
}

// ---------------------------------------------------------------------------
// Shared metadata struct (crawler writes, uploader & patcher read)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// main – spin up the three‑stage pipeline
// ---------------------------------------------------------------------------

func main() {
	flag.Parse()
	if startURL == "" {
		log.Fatal("-url is required")
	}

	_ = os.RemoveAll(outDir)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	paths := make(chan string, 128) // crawler → uploader
	doneUpload := make(chan struct{})

	var wg sync.WaitGroup

	// 1) Crawler
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Use the new configurable crawler with custom settings
		config := steps.Config{
			HTTPTimeout: httpTimeout,
			MaxDepth:    maxDepth,
			Verbose:     verbose,
		}
		crawler := steps.NewCrawler(config)
		crawler.Run(context.Background(), startURL, outDir, paths)
	}()

	// 2) Uploader
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Use the new configurable uploader with custom settings
		uploaderConfig := steps.UploaderConfig{
			ProjectID:   projectID,
			DriveFolder: driveFolder,
			Verbose:     verbose,
		}
		uploader, err := steps.NewUploader(context.Background(), uploaderConfig)
		if err != nil {
			log.Printf("FATAL: failed to create uploader: %v", err)
			close(doneUpload)
			return
		}
		if err := uploader.Run(context.Background(), outDir, paths, doneUpload); err != nil {
			log.Printf("FATAL: uploader failed: %v", err)
		}
	}()

	// 3) Patcher (starts once uploader signals done)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Use the new configurable patcher with custom settings
		patcherConfig := steps.PatcherConfig{
			ProjectID:        projectID,
			Verbose:          verbose,
			RateLimitDelay:   1100 * time.Millisecond, // Stay under 60 req/min
			MaxRetryAttempts: 6,
		}
		patcher, err := steps.NewPatcher(context.Background(), patcherConfig)
		if err != nil {
			log.Printf("FATAL: failed to create patcher: %v", err)
			return
		}
		if err := patcher.Run(context.Background(), outDir, doneUpload); err != nil {
			log.Printf("FATAL: patcher failed: %v", err)
			return
		}
		log.Println("patcher done")
	}()

	wg.Wait()
	log.Println("✓ pipeline complete")
}

// ---------------------------------------------------------------------------
// CRAWLER (unchanged apart from sending dir paths)
// ---------------------------------------------------------------------------
