package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	tzfs "github.com/temporalio/temporal-zfs/pkg/fs"
	"github.com/temporalio/temporal-zfs/pkg/store"
	"go.temporal.io/sdk/activity"
)

// Activities holds the shared store and implements the 5 research agent activities.
// Each activity opens an isolated TemporalZFS partition, reads prior state from the
// previous step's snapshot (guaranteeing a consistent view even if a prior attempt
// left partial writes in HEAD), writes new files, and creates a CoW snapshot.
type Activities struct {
	baseStore store.Store
	stats     *RunStats            // shared stats for real-time dashboard updates
	eventCh   chan<- WorkflowEvent // per-activity events for the dashboard
}

// emitEvent sends a dashboard event for the current activity step.
func (a *Activities) emitEvent(ctx context.Context, params WorkflowParams, stepIndex int, stepName, state string) {
	if a.eventCh == nil {
		return
	}
	select {
	case a.eventCh <- WorkflowEvent{
		TopicSlug: params.TopicSlug,
		StepIndex: stepIndex,
		StepName:  stepName,
		State:     state,
		Attempt:   int(activity.GetInfo(ctx).Attempt),
		Timestamp: time.Now(),
	}:
	default: // don't block if channel is full
	}
}

// openFS opens an existing FS for the workflow's partition.
func (a *Activities) openFS(partitionID uint64) (*tzfs.FS, error) {
	s := store.NewPrefixedStore(a.baseStore, partitionID)
	f, err := tzfs.Open(s)
	if err != nil {
		return nil, fmt.Errorf("open fs: %w", err)
	}
	return f, nil
}

// onRetry records a retry in shared stats and logs the recovery with prior state info.
func (a *Activities) onRetry(ctx context.Context, priorFiles int, priorSnapshot string) {
	a.stats.Retries.Add(1)
	activity.GetLogger(ctx).Info("Retrying with durable FS state intact",
		"attempt", activity.GetInfo(ctx).Attempt,
		"filesFromPriorStep", priorFiles,
		"lastSnapshot", priorSnapshot,
	)
}

// retries returns the number of retries for the current activity execution.
func retries(ctx context.Context) int {
	info := activity.GetInfo(ctx)
	if info.Attempt > 1 {
		return int(info.Attempt) - 1
	}
	return 0
}

// maybeFail injects a random failure based on the configured failure rate.
// It incorporates the attempt number so retries can succeed after earlier failures.
func maybeFail(ctx context.Context, seed int64, rate float64, msg string) error {
	attempt := int64(activity.GetInfo(ctx).Attempt)
	r := rand.New(rand.NewSource(seed + attempt*1000))
	if rate > 0 && r.Float64() < rate {
		return errors.New(msg)
	}
	return nil
}

// countFiles counts files in a directory (non-recursive).
func countFiles(f *tzfs.FS, dir string) int {
	entries, err := f.ReadDir(dir)
	if err != nil {
		return 0
	}
	count := 0
	for _, e := range entries {
		if e.Type != tzfs.InodeTypeDir {
			count++
		}
	}
	return count
}

// WebResearch simulates gathering research sources: creates workspace dirs
// and writes 3-5 source files. Failure rate: 20% * multiplier.
func (a *Activities) WebResearch(ctx context.Context, params WorkflowParams) (StepResult, error) {
	a.emitEvent(ctx, params, 0, "WebResearch", "started")
	f, err := a.openFS(params.PartitionID)
	if err != nil {
		return StepResult{}, err
	}
	defer f.Close()

	// On retry: verify FS opened successfully (partition is durable).
	if activity.GetInfo(ctx).Attempt > 1 {
		a.emitEvent(ctx, params, 0, "WebResearch", "retrying")
		a.onRetry(ctx, 0, "(none — first step)")
	}

	// Inject failure AFTER opening FS — proves partition survives failures.
	if err := maybeFail(ctx, params.Seed+1, 0.20*params.FailureRate, "simulated web API timeout"); err != nil {
		return StepResult{}, err
	}

	// Create workspace directories (idempotent — ignore ErrExist).
	for _, dir := range []string{
		"/research",
		"/research/" + params.TopicSlug,
		"/research/" + params.TopicSlug + "/sources",
	} {
		if mkErr := f.Mkdir(dir, 0o755); mkErr != nil && !errors.Is(mkErr, tzfs.ErrExist) {
			return StepResult{}, fmt.Errorf("mkdir %s: %w", dir, mkErr)
		}
	}

	// Generate and write source files.
	sources := generateSources(params.TopicName, params.Seed)
	var result StepResult
	for _, src := range sources {
		path := "/research/" + params.TopicSlug + "/sources/" + src.Filename
		if err := f.WriteFile(path, src.Content, 0o644); err != nil {
			return StepResult{}, fmt.Errorf("write %s: %w", path, err)
		}
		result.FilesCreated++
		result.BytesWritten += int64(len(src.Content))
	}

	// Snapshot after this step.
	if _, err := f.CreateSnapshot("step-1-research"); err != nil && !errors.Is(err, tzfs.ErrExist) {
		return StepResult{}, fmt.Errorf("snapshot: %w", err)
	}

	result.Retries = retries(ctx)
	a.emitEvent(ctx, params, 0, "WebResearch", "completed")
	return result, nil
}

// Summarize reads all source files and produces a summary. Failure rate: 15%.
func (a *Activities) Summarize(ctx context.Context, params WorkflowParams) (StepResult, error) {
	a.emitEvent(ctx, params, 1, "Summarize", "started")
	f, err := a.openFS(params.PartitionID)
	if err != nil {
		return StepResult{}, err
	}
	defer f.Close()

	// Open step-1 snapshot for reads — guaranteed consistent view even if a
	// prior attempt left partial writes in HEAD.
	snapFS, err := f.OpenSnapshot("step-1-research")
	if err != nil {
		return StepResult{}, fmt.Errorf("open snapshot step-1-research: %w", err)
	}
	defer snapFS.Close()

	// Read source filenames from snapshot — verifies step 1's files survived.
	sourcesDir := "/research/" + params.TopicSlug + "/sources"
	entries, err := snapFS.ReadDir(sourcesDir)
	if err != nil {
		return StepResult{}, fmt.Errorf("readdir %s: %w", sourcesDir, err)
	}

	// On retry: step 1's source files are still here — read from snapshot, not HEAD.
	if activity.GetInfo(ctx).Attempt > 1 {
		a.emitEvent(ctx, params, 1, "Summarize", "retrying")
		a.onRetry(ctx, len(entries), "step-1-research")
	}

	// Inject failure AFTER verifying prior state.
	if err := maybeFail(ctx, params.Seed+2, 0.15*params.FailureRate, "simulated LLM rate limit exceeded"); err != nil {
		return StepResult{}, err
	}

	sourceNames := make([]string, len(entries))
	for i, e := range entries {
		sourceNames[i] = e.Name
	}

	// Generate and write summary.
	content := generateSummary(params.TopicName, sourceNames, params.Seed)
	path := "/research/" + params.TopicSlug + "/summary.md"
	if err := f.WriteFile(path, content, 0o644); err != nil {
		return StepResult{}, fmt.Errorf("write summary: %w", err)
	}

	if _, err := f.CreateSnapshot("step-2-summary"); err != nil && !errors.Is(err, tzfs.ErrExist) {
		return StepResult{}, fmt.Errorf("snapshot: %w", err)
	}

	a.emitEvent(ctx, params, 1, "Summarize", "completed")
	return StepResult{FilesCreated: 1, BytesWritten: int64(len(content)), Retries: retries(ctx)}, nil
}

// FactCheck reads the summary and produces a fact-check report. Failure rate: 10%.
func (a *Activities) FactCheck(ctx context.Context, params WorkflowParams) (StepResult, error) {
	a.emitEvent(ctx, params, 2, "FactCheck", "started")
	f, err := a.openFS(params.PartitionID)
	if err != nil {
		return StepResult{}, err
	}
	defer f.Close()

	// Open step-2 snapshot — read prior state from known-good point.
	topicDir := "/research/" + params.TopicSlug
	snapFS, err := f.OpenSnapshot("step-2-summary")
	if err != nil {
		return StepResult{}, fmt.Errorf("open snapshot step-2-summary: %w", err)
	}
	priorFiles := countFiles(snapFS, topicDir)
	snapFS.Close()

	// On retry: summary + sources from prior steps verified via snapshot.
	if activity.GetInfo(ctx).Attempt > 1 {
		a.emitEvent(ctx, params, 2, "FactCheck", "retrying")
		a.onRetry(ctx, priorFiles, "step-2-summary")
	}

	// Inject failure AFTER verifying prior state.
	if err := maybeFail(ctx, params.Seed+3, 0.10*params.FailureRate, "simulated fact-checking service unavailable"); err != nil {
		return StepResult{}, err
	}

	content := generateFactCheck(params.TopicName, params.Seed)
	path := topicDir + "/fact-check.md"
	if err := f.WriteFile(path, content, 0o644); err != nil {
		return StepResult{}, fmt.Errorf("write fact-check: %w", err)
	}

	if _, err := f.CreateSnapshot("step-3-factcheck"); err != nil && !errors.Is(err, tzfs.ErrExist) {
		return StepResult{}, fmt.Errorf("snapshot: %w", err)
	}

	a.emitEvent(ctx, params, 2, "FactCheck", "completed")
	return StepResult{FilesCreated: 1, BytesWritten: int64(len(content)), Retries: retries(ctx)}, nil
}

// FinalReport reads all artifacts and produces a final report. Failure rate: 10%.
func (a *Activities) FinalReport(ctx context.Context, params WorkflowParams) (StepResult, error) {
	a.emitEvent(ctx, params, 3, "FinalReport", "started")
	f, err := a.openFS(params.PartitionID)
	if err != nil {
		return StepResult{}, err
	}
	defer f.Close()

	// Open step-3 snapshot — read prior state from known-good point.
	topicDir := "/research/" + params.TopicSlug
	snapFS, err := f.OpenSnapshot("step-3-factcheck")
	if err != nil {
		return StepResult{}, fmt.Errorf("open snapshot step-3-factcheck: %w", err)
	}
	priorFiles := countFiles(snapFS, topicDir)
	snapFS.Close()

	// On retry: sources + summary + fact-check verified via snapshot.
	if activity.GetInfo(ctx).Attempt > 1 {
		a.emitEvent(ctx, params, 3, "FinalReport", "retrying")
		a.onRetry(ctx, priorFiles, "step-3-factcheck")
	}

	// Inject failure AFTER verifying prior state.
	if err := maybeFail(ctx, params.Seed+4, 0.10*params.FailureRate, "simulated context window exceeded"); err != nil {
		return StepResult{}, err
	}

	content := generateFinalReport(params.TopicName, params.Seed)
	path := topicDir + "/report.md"
	if err := f.WriteFile(path, content, 0o644); err != nil {
		return StepResult{}, fmt.Errorf("write report: %w", err)
	}

	if _, err := f.CreateSnapshot("step-4-report"); err != nil && !errors.Is(err, tzfs.ErrExist) {
		return StepResult{}, fmt.Errorf("snapshot: %w", err)
	}

	a.emitEvent(ctx, params, 3, "FinalReport", "completed")
	return StepResult{FilesCreated: 1, BytesWritten: int64(len(content)), Retries: retries(ctx)}, nil
}

// PeerReview reads the report and produces a peer review. Failure rate: 5%.
func (a *Activities) PeerReview(ctx context.Context, params WorkflowParams) (StepResult, error) {
	a.emitEvent(ctx, params, 4, "PeerReview", "started")
	f, err := a.openFS(params.PartitionID)
	if err != nil {
		return StepResult{}, err
	}
	defer f.Close()

	// Open step-4 snapshot — read prior state from known-good point.
	topicDir := "/research/" + params.TopicSlug
	snapFS, err := f.OpenSnapshot("step-4-report")
	if err != nil {
		return StepResult{}, fmt.Errorf("open snapshot step-4-report: %w", err)
	}
	priorFiles := countFiles(snapFS, topicDir)
	snapFS.Close()

	// On retry: all artifacts from prior steps verified via snapshot.
	if activity.GetInfo(ctx).Attempt > 1 {
		a.emitEvent(ctx, params, 4, "PeerReview", "retrying")
		a.onRetry(ctx, priorFiles, "step-4-report")
	}

	// Inject failure AFTER verifying prior state.
	if err := maybeFail(ctx, params.Seed+5, 0.05*params.FailureRate, "simulated reviewer model overloaded"); err != nil {
		return StepResult{}, err
	}

	content := generatePeerReview(params.TopicName, params.Seed)
	path := topicDir + "/review.md"
	if err := f.WriteFile(path, content, 0o644); err != nil {
		return StepResult{}, fmt.Errorf("write review: %w", err)
	}

	if _, err := f.CreateSnapshot("step-5-review"); err != nil && !errors.Is(err, tzfs.ErrExist) {
		return StepResult{}, fmt.Errorf("snapshot: %w", err)
	}

	a.emitEvent(ctx, params, 4, "PeerReview", "completed")
	return StepResult{FilesCreated: 1, BytesWritten: int64(len(content)), Retries: retries(ctx)}, nil
}
