package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/rasha-hantash/gdoc-pipeline/lib/logger"
	"github.com/rasha-hantash/gdoc-pipeline/pipeline"
	"github.com/rasha-hantash/gdoc-pipeline/steps/crawler"
	"github.com/rasha-hantash/gdoc-pipeline/steps/uploader"
	"github.com/rasha-hantash/gdoc-pipeline/steps/patcher"

	"google.golang.org/api/docs/v1"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
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

		// --- build shared Google API clients ------------------------------------
		var opts []option.ClientOption
		if projectID != "" {
			opts = append(opts, option.WithQuotaProject(projectID))
		}
	
		docsSvc, err := docs.NewService(ctx, opts...)
		if err != nil {
			slog.Error("failed to create Docs service", slog.Any("error", err))
			return
		}
	
		sheetsSvc, err := sheets.NewService(ctx, opts...)
		if err != nil {
			slog.Error("failed to create Sheets service", slog.Any("error", err))
			return
		}	

	// instantiate the crawler, uploader, and patcher
	crawler := crawler.NewCrawler(depth, 15*time.Second, url, out, docsSvc, sheetsSvc)
	
	uploader, err := uploader.NewUploader(ctx, projectID, driveFolder, out)
	if err != nil {
		slog.Error("failed to create uploader", slog.Any("error", err))
		os.Exit(1)
	}
	patcher, err := patcher.NewPatcher(ctx, projectID, 1100*time.Millisecond, 6, out)
	if err != nil {
		slog.Error("failed to create patcher", slog.Any("error", err))
		os.Exit(1)
	}

	steps := []pipeline.Step{
		crawler,
		uploader,
		patcher,
	}

	pipe := pipeline.NewPipeline(steps...)

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

