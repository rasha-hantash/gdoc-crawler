package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/rasha-hantash/gdoc-pipeline/lib/logger"
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
		// timeout     time.Duration
	)

	flag.StringVar(&url, "url", "", "root Google Doc URL to crawl")
	flag.StringVar(&out, "out", "./out", "output directory")
	flag.IntVar(&depth, "depth", 5, "crawl depth")
	flag.StringVar(&retry, "retry", "", "name of the step to retry (crawler|uploader|patcher)")
	// flag.DurationVar(&timeout, "timeout", 60*time.Minute, "overall pipeline timeout (0 = none)")
	flag.StringVar(&projectID, "project", "", "GCP quota-project (optional)")
	flag.StringVar(&driveFolder, "folder", "Imported Docs", "Drive folder (created if absent)")
	flag.Parse()

	if url == "" {
		slog.Error("url flag is required")
		os.Exit(1)
	}

	ctx := context.Background()
	// load configuration
	slogHandler := &logger.ContextHandler{Handler: slog.NewJSONHandler(os.Stdout, nil)}
	slog.SetDefault(slog.New(slogHandler))

	slog.Info("starting pipeline",
		slog.String("url", url),
		slog.String("output_dir", out),
		slog.Int("max_depth", depth))

	// instantiate the crawler, uploader, and patcher
	crawler := steps.NewCrawler(depth, 15*time.Second, url, out)
	
	uploader, err := steps.NewUploader(ctx, projectID, driveFolder, out)
	if err != nil {
		slog.Error("failed to create uploader", slog.Any("error", err))
		os.Exit(1)
	}
	patcher, err := steps.NewPatcher(ctx, projectID, 1100*time.Millisecond, 6, out)
	if err != nil {
		slog.Error("failed to create patcher", slog.Any("error", err))
		os.Exit(1)
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
			slog.Error("unknown step",
				slog.String("step", retry),
				slog.String("valid_values", "crawler, uploader, patcher"))
			os.Exit(1)
		}
	}

	if err := pipe.RunFrom(ctx, idx); err != nil {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			slog.Error("filesystem error", slog.Any("error", pathErr))
			os.Exit(1)
		}
		slog.Error("pipeline failed", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("pipeline completed successfully")
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
		slog.Info("running step",
			slog.String("step", step.Name()),
			slog.Int("current", i+1),
			slog.Int("total", len(p.steps)))
		t0 := time.Now()

		if err := step.Run(ctx); err != nil {
			return fmt.Errorf("step %s failed after %s: %w", step.Name(), time.Since(t0).Truncate(time.Millisecond), err)
		}

		slog.Info("completed step",
			slog.String("step", step.Name()),
			slog.Duration("duration", time.Since(t0).Truncate(time.Millisecond)))
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
