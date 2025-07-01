package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

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
