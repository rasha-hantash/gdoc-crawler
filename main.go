// // gdoc_pipeline.go — concurrent crawler + Drive uploader + hyperlink patcher
// //
// // Three goroutines coordinate via two channels:
// //   • crawler   — walks a public Google Doc/Sheet graph, writes each page to
// //                 disk and sends its directory path over the paths channel.
// //   • uploader  — receives directory paths, uploads the file (HTML/CSV) to
// //                 Drive, builds id_map.json, then closes doneUpload.
// //   • patcher   — waits for doneUpload (so id_map.json is available), then
// //                 scans every crawled Doc and uses the Docs API to rewrite
// //                 internal hyperlinks to point at the newly‑uploaded copies.
// //
// // Usage (all flags are optional aside from -url):
// //   go run gdoc_pipeline.go \
// //        -url     "https://docs.google.com/document/d/XXXXXXXX/edit" \
// //        -out     ./out \
// //        -depth   8 \
// //        -httptimeout 15s \
// //        -folder  "Imported Docs" \
// //        -project "my-gcp-project" \
// //        -v
// //
// // Notes
// // -----
// // * The uploader throttles Drive API automatically via the Docs client‑side
// //   library; patcher adds explicit back‑off for 503 errors.
// // * For clarity this is still a single file. In production, consider splitting
// //   into packages (crawler, driveutil, patcher) and adding context cancelation.

// package main

// import (
// 	"context"
// 	"flag"
// 	"log"
// 	"os"
// 	"os/signal"
// 	"syscall"
// 	"time"

// 	"github.com/rasha-hantash/gdoc-pipeline/steps"
// )

// // ---------------------------------------------------------------------------
// // CLI flags (union of the three original tools)
// // ---------------------------------------------------------------------------
// var (
// 	// Crawler
// 	startURL    string
// 	outDir      string
// 	maxDepth    int
// 	httpTimeout time.Duration
// 	step        string

// 	// Uploader / Patcher
// 	driveFolder string
// 	projectID   string

// 	// Misc
// 	verbose bool
// )

// func init() {
// 	flag.StringVar(&startURL, "url", "", "Public Google Doc/Sheet URL to start crawling from")
// 	flag.StringVar(&outDir, "out", "./out", "Output directory")
// 	flag.IntVar(&maxDepth, "depth", 5, "Maximum depth for nested Docs/Sheets")
// 	flag.DurationVar(&httpTimeout, "httptimeout", 15*time.Second, "HTTP timeout per request")
// 	flag.StringVar(&step, "step", "all", "Step to run (all, uploader, patcher)")

// 	flag.StringVar(&driveFolder, "folder", "Imported Docs", "Drive folder (created if absent)")
// 	flag.StringVar(&projectID, "project", "", "GCP quota-project (optional)")

// 	flag.BoolVar(&verbose, "v", false, "Verbose logging")
// 	flag.BoolVar(&verbose, "verbose", false, "Verbose logging (alias of -v)")
// }

// // ---------------------------------------------------------------------------
// // Shared metadata struct (crawler writes, uploader & patcher read)
// // ---------------------------------------------------------------------------

// // ---------------------------------------------------------------------------
// // main – spin up the three‑stage pipeline
// // ---------------------------------------------------------------------------

// // todo simplify to one channel?
// // func main() {
// // 	flag.Parse()
// // 	if startURL == "" {
// // 		log.Fatal("-url is required")
// // 	}
// // 	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
// //     defer stop()

// // 	_ = os.RemoveAll(outDir)
// // 	if err := os.MkdirAll(outDir, 0o755); err != nil {
// // 		log.Fatalf("Failed to create output directory: %v", err)
// // 	}

// // 	paths := make(chan string, 128) // crawler → uploader
// // 	doneUpload := make(chan struct{})

// // 	var wg sync.WaitGroup

// // 	// 1) Crawler
// // 	wg.Add(1)
// // 	go func() {
// // 		defer wg.Done()
// // 		// Use the new configurable crawler with custom settings
// // 		config := steps.Config{
// // 			HTTPTimeout: httpTimeout,
// // 			MaxDepth:    maxDepth,
// // 			Verbose:     verbose,
// // 		}
// // 		crawler := steps.NewCrawler(config)
// // 		crawler.Run(ctx, startURL, outDir, paths)
// // 	}()

// // 	// 2) Uploader
// // 	wg.Add(1)
// // 	go func() {
// // 		defer wg.Done()
// // 		// Use the new configurable uploader with custom settings
// // 		uploaderConfig := steps.UploaderConfig{
// // 			ProjectID:   projectID,
// // 			DriveFolder: driveFolder,
// // 			Verbose:     verbose,
// // 		}
// // 		uploader, err := steps.NewUploader(ctx, uploaderConfig)
// // 		if err != nil {
// // 			log.Printf("FATAL: failed to create uploader: %v", err)
// // 			close(doneUpload)
// // 			return
// // 		}
// // 		if err := uploader.Run(ctx, outDir, paths, doneUpload); err != nil {
// // 			log.Printf("FATAL: uploader failed: %v", err)
// // 		}
// // 	}()

// // 	// 3) Patcher (starts once uploader signals done)
// // 	wg.Add(1)
// // 	go func() {
// // 		defer wg.Done()
// // 		// Use the new configurable patcher with custom settings
// // 		patcherConfig := steps.PatcherConfig{
// // 			ProjectID:        projectID,
// // 			Verbose:          verbose,
// // 			RateLimitDelay:   1100 * time.Millisecond, // Stay under 60 req/min
// // 			MaxRetryAttempts: 6,
// // 		}
// // 		patcher, err := steps.NewPatcher(ctx, patcherConfig)
// // 		if err != nil {
// // 			log.Printf("FATAL: failed to create patcher: %v", err)
// // 			return
// // 		}
// // 		if err := patcher.Run(ctx, outDir, doneUpload); err != nil {
// // 			log.Printf("FATAL: patcher failed: %v", err)
// // 			return
// // 		}
// // 		log.Println("patcher done")
// // 	}()

// // 	wg.Wait()
// // 	log.Println("✓ pipeline complete")
// // }

// type PipelineConfig struct {
// 	StartURL    string
// 	Step        string
// 	OutDir      string
// 	MaxDepth    int
// 	HTTPTimeout time.Duration
// 	DriveFolder string
// }

// type PipelineMessage struct {
// 	Step    string
// 	OutDirPath string
// }

// func main() {
// 	flag.Parse()
// 	if startURL == "" {
// 		log.Fatal("-url is required")
// 	}
// 	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
// 	defer stop()

// 	_ = os.RemoveAll(outDir)
// 	if err := os.MkdirAll(outDir, 0o755); err != nil {
// 		log.Fatalf("Failed to create output directory: %v", err)
// 	}

// 	pipelineCfg := PipelineConfig{
// 		StartURL:    startURL,
// 		Step:        step,
// 		OutDir:      outDir,
// 		MaxDepth:    maxDepth,
// 		HTTPTimeout: httpTimeout,
// 		DriveFolder: driveFolder,
// 	}

// 	// instantiate the crawler, uploader, and patcher
// 	crawler := steps.NewCrawler(steps.Config{
// 		HTTPTimeout: pipelineCfg.HTTPTimeout,
// 		MaxDepth:    pipelineCfg.MaxDepth,
// 		Verbose:     verbose,
// 	})
// 	uploader, err := steps.NewUploader(ctx, steps.UploaderConfig{
// 		ProjectID:   projectID,
// 		DriveFolder: pipelineCfg.DriveFolder,
// 		Verbose:     verbose,
// 	})
// 	if err != nil {
// 		log.Fatalf("Failed to create uploader: %v", err)
// 	}
// 	patcher, err := steps.NewPatcher(ctx, steps.PatcherConfig{
// 		ProjectID: projectID,
// 		Verbose:   verbose,
// 	})
// 	if err != nil {
// 		log.Fatalf("Failed to create patcher: %v", err)
// 	}

// 	go runPipeline(ctx, pipelineCfg, crawler, uploader, patcher)

// }
// func runPipeline(ctx context.Context, pipelineCfg PipelineConfig, crawler *steps.Crawler, uploader *steps.Uploader, patcher *steps.Patcher) {
// 	pipelineMsg := make(chan PipelineMessage, 10)

// 	pipelineMsg <- PipelineMessage{Step: "crawler", OutDirPath: pipelineCfg.OutDir}

// 	for msg := range pipelineMsg {
// 		switch msg.Step {
// 		case "crawler":
// 			crawler.Run(ctx, pipelineCfg.StartURL,pipelineMsg)
// 		case "uploader":
// 			uploader.Run(ctx, pipelineMsg)
// 		case "patcher":
// 			patcher.Run(ctx, pipelineMsg)
// 		default:
// 			log.Fatalf("Invalid step: %s", msg)
// 		}
// 	}

// }

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rasha-hantash/gdoc-pipeline/steps"
)

// -----------------------------------------------------------------------------
// CLI entry‑point
// -----------------------------------------------------------------------------

func main() {
	var (
		url         string
		out         string
		depth       int
		retry       string
		projectID   string
		driveFolder string
		verbose     bool
		// timeout     time.Duration
	)

	flag.StringVar(&url, "url", "", "root Google Doc URL to crawl")
	flag.StringVar(&out, "out", "./out", "output directory")
	flag.IntVar(&depth, "depth", 5, "crawl depth")
	flag.StringVar(&retry, "retry", "", "name of the step to retry (crawler|uploader|patcher)")
	// flag.DurationVar(&timeout, "timeout", 60*time.Minute, "overall pipeline timeout (0 = none)")
	flag.StringVar(&projectID, "project", "", "GCP quota-project (optional)")
	flag.StringVar(&driveFolder, "folder", "Imported Docs", "Drive folder (created if absent)")
	flag.BoolVar(&verbose, "v", false, "Verbose logging")
	flag.BoolVar(&verbose, "verbose", false, "Verbose logging (alias of -v)")
	flag.Parse()

	if url == "" {
		log.Fatal("‑url flag is required")
	}

	ctx := context.Background()

	// instantiate the crawler, uploader, and patcher
	crawler := steps.NewCrawler(steps.Config{
		HTTPTimeout: 15 * time.Second,
		MaxDepth:    depth,
		Verbose:     verbose,
	}, url, out)
	uploader, err := steps.NewUploader(ctx, steps.UploaderConfig{
		ProjectID:   projectID,
		DriveFolder: driveFolder,
		Verbose:     verbose,
	}, out)
	if err != nil {
		log.Fatalf("Failed to create uploader: %v", err)
	}
	patcher, err := steps.NewPatcher(ctx, steps.PatcherConfig{
		ProjectID:        projectID,
		Verbose:          verbose,
		RateLimitDelay:   1100 * time.Millisecond, // Stay under 60 req/min
		MaxRetryAttempts: 6,
	}, out)
	if err != nil {
		log.Fatalf("Failed to create patcher: %v", err)
	}

	steps := []Step{
		crawler,
		uploader,
		patcher,
	}

	pipe := NewPipeline(steps...)

	idx := 0
	if retry != "" {
		idx = pipe.FindIndex(retry)
		if idx == -1 {
			log.Fatalf("unknown step %q — valid values: crawler, uploader, patcher", retry)
		}
	}

	if err := pipe.RunFrom(ctx, idx); err != nil {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			log.Fatalf("filesystem error: %v", pathErr)
		}
		log.Fatalf("pipeline failed: %v", err)
	}

	log.Print("[pipeline] ✅ all steps completed successfully")
}


// Step represents a discrete unit of work in the pipeline.
// Every step must be idempotent so it can safely be re‑executed.
type Step interface {
	Name() string
	Run(ctx context.Context) error
}

// Pipeline orchestrates a fixed list of steps.
type Pipeline struct {
	steps []Step
}

func NewPipeline(steps ...Step) *Pipeline {
	return &Pipeline{steps: steps}
}

// RunFrom executes steps starting at the provided index.
// If any step returns an error, execution stops and the error bubbles up.
func (p *Pipeline) RunFrom(ctx context.Context, start int) error {
	if start < 0 || start >= len(p.steps) {
		return fmt.Errorf("start index %d out of range", start)
	}

	for i := start; i < len(p.steps); i++ {
		step := p.steps[i]
		log.Printf("[pipeline] ▶ running step %s (%d/%d)", step.Name(), i+1, len(p.steps))
		t0 := time.Now()

		if err := step.Run(ctx); err != nil {
			return fmt.Errorf("step %s failed after %s: %w", step.Name(), time.Since(t0).Truncate(time.Millisecond), err)
		}

		log.Printf("[pipeline] ✓ completed %s in %s", step.Name(), time.Since(t0).Truncate(time.Millisecond))
	}

	return nil
}

// FindIndex returns the position of a step by name or ‑1 if not found.
func (p *Pipeline) FindIndex(name string) int {
	for i, s := range p.steps {
		if s.Name() == name {
			return i
		}
	}
	return -1
}
