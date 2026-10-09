package branchsync

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/gate"
	gitpkg "github.com/kunchenguid/no-mistakes/internal/git"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

// newStaleMirrorAdoptionFixture is the authorized-rewrite adoption state with
// the private mirror lane where a real terminal run leaves it: still at the
// submitted head. The pipeline's rewritten result exists in the gate only
// under its run-specific recovery anchor.
func newStaleMirrorAdoptionFixture(t *testing.T) *recoverFixture {
	t.Helper()
	f := newAuthorizedRewriteRecoverFixture(t)
	mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, f.submitted, f.preserved)
	return f
}

func (f *recoverFixture) mirrorLane() string {
	f.t.Helper()
	return readGitOptional(f.t, f.gate, "refs/heads/"+f.run.Branch)
}

func readGitOptional(t *testing.T, dir, ref string) string {
	t.Helper()
	out, err := gitpkg.Run(context.Background(), dir, "rev-parse", "--verify", "--quiet", ref)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// assertNextRunMirrorReady drives the real private-mirror preparation a fresh
// `axi run` performs, for both the adopted head itself and a later operator
// commit on top of it, and proves the superseded submitted head is still
// reachable from the gate afterwards.
func assertNextRunMirrorReady(t *testing.T, f *recoverFixture) {
	t.Helper()
	if got := mustRun(t, f.gate, "rev-parse", f.localAnchorRef()); got != f.submitted {
		t.Fatalf("gate caller-head anchor = %s, want submitted %s", got, f.submitted)
	}
	if got := mustRun(t, f.gate, "rev-parse", f.anchorRef()); got != f.preserved {
		t.Fatalf("gate preserved anchor = %s, want %s", got, f.preserved)
	}
	reconciled, err := gate.ReconcileStaleBranch(f.ctx, f.gate, f.local, f.run.Branch, f.preserved, "")
	if err != nil || reconciled.Reconciled {
		t.Fatalf("next-run mirror preparation for the adopted head = %#v, err %v", reconciled, err)
	}

	mustWrite(t, filepath.Join(f.local, "next.txt"), "next run work\n")
	mustRun(t, f.local, "add", "next.txt")
	mustRun(t, f.local, "commit", "-m", "next run work")
	next := mustRun(t, f.local, "rev-parse", "HEAD")
	reconciled, err = gate.ReconcileStaleBranch(f.ctx, f.gate, f.local, f.run.Branch, next, "")
	if err != nil || reconciled.Reconciled {
		t.Fatalf("next-run mirror preparation for a later commit = %#v, err %v", reconciled, err)
	}
	// The submission itself is now an ordinary fast-forward: no force.
	mustRun(t, f.local, "push", f.gate, "refs/heads/"+f.run.Branch+":refs/heads/"+f.run.Branch)
	if got := f.mirrorLane(); got != next {
		t.Fatalf("fast-forward submission left the lane at %s, want %s", got, next)
	}
	mustRun(t, f.gate, "cat-file", "-e", f.submitted+"^{commit}")
	if got := mustRun(t, f.gate, "rev-parse", f.localAnchorRef()); got != f.submitted {
		t.Fatalf("submitted head anchor moved to %s after the next submission", got)
	}
}

func TestAdoptTerminalHeadSettlesPrivateMirrorForTheNextRun(t *testing.T) {
	t.Parallel()

	f := newStaleMirrorAdoptionFixture(t)
	if automatic := f.service.Recover(f.ctx, false); automatic.Recovered || automatic.Safety != "blocked_recover_diverged" {
		t.Fatalf("automatic recovery crossed the explicit-authorization boundary: %#v", automatic)
	}
	if got := f.mirrorLane(); got != f.submitted {
		t.Fatalf("automatic refusal moved the mirror lane to %s", got)
	}

	state := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f))
	if !state.Recovered || !state.Changed || state.State != StateCustodyReturned {
		t.Fatalf("authorized terminal-head adoption = %#v", state)
	}
	assertRecoveredRewriteFixture(t, f)
	if got := f.mirrorLane(); got != f.preserved {
		t.Fatalf("mirror lane = %s, want adopted head %s", got, f.preserved)
	}
	if status := f.service.InspectCached(f.ctx); status.State != StateCustodyReturned || status.Safety != "custody_returned" || status.NextAction == nil || status.NextAction.Code != "run_pipeline" {
		t.Fatalf("status after a settled adoption = %#v", status)
	}
	if replay := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)); !replay.Recovered || replay.Changed {
		t.Fatalf("idempotent replay = %#v", replay)
	}
	assertNextRunMirrorReady(t, f)
}

// TestAdoptTerminalHeadReplaySettlesMirrorLeftByEarlierAdoption reproduces
// the reported follow-through defect: an adoption completed custody while the
// private mirror lane still named the submitted head, so the next run's mirror
// guard refused that head as at-risk. Replaying the same fully bound action
// settles the lane, and the guard itself is unchanged: the gate's caller-head
// anchor is not preservation credit.
func TestAdoptTerminalHeadReplaySettlesMirrorLeftByEarlierAdoption(t *testing.T) {
	t.Parallel()

	f := newStaleMirrorAdoptionFixture(t)
	request := terminalHeadAdoptionRequest(f)
	if state := f.service.AdoptTerminalHead(f.ctx, request); !state.Recovered {
		t.Fatalf("adoption = %#v", state)
	}
	// Recreate the state an adoption without mirror settlement left behind.
	mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, f.submitted, f.preserved)

	_, err := gate.ReconcileStaleBranch(f.ctx, f.gate, f.local, f.run.Branch, f.preserved, "")
	if err == nil || !strings.Contains(err.Error(), "at-risk") || !strings.Contains(err.Error(), f.submitted) {
		t.Fatalf("stale lane with the caller anchor present must still refuse as at-risk, got %v", err)
	}
	mustRun(t, f.gate, "update-ref", "-d", f.localAnchorRef(), f.submitted)
	_, err = gate.ReconcileStaleBranch(f.ctx, f.gate, f.local, f.run.Branch, f.preserved, "")
	if err == nil || !strings.Contains(err.Error(), "at-risk") {
		t.Fatalf("reproduction: stale lane must refuse the adopted head, got %v", err)
	}
	if got := f.mirrorLane(); got != f.submitted {
		t.Fatalf("refused preparation moved the lane to %s", got)
	}

	if status := f.service.InspectCached(f.ctx); !TerminalAdoptionUnsettled(status) || status.NextAction == nil || status.NextAction.Code != "adopt_terminal_head" {
		t.Fatalf("stale-lane status before replay = %#v", status)
	}
	replay := f.service.AdoptTerminalHead(f.ctx, request)
	if !replay.Recovered || replay.Changed {
		t.Fatalf("replay settling the mirror = %#v", replay)
	}
	if TerminalAdoptionUnsettled(replay) || replay.Error != "" || replay.NextAction == nil || replay.NextAction.Code != "run_pipeline" {
		t.Fatalf("successful replay still reports the superseded replay offer: %#v", replay)
	}
	if got := f.mirrorLane(); got != f.preserved {
		t.Fatalf("replay left the mirror lane at %s", got)
	}
	assertNextRunMirrorReady(t, f)
}

// TestAdoptTerminalHeadReplayRefusesAMovedLaneWithoutDenyingCustody keeps the
// replay of a completed adoption fail-closed against a lane someone else moved
// while reporting accurately that custody was already returned.
func TestAdoptTerminalHeadReplayRefusesAMovedLaneWithoutDenyingCustody(t *testing.T) {
	t.Parallel()

	f := newStaleMirrorAdoptionFixture(t)
	request := terminalHeadAdoptionRequest(f)
	if state := f.service.AdoptTerminalHead(f.ctx, request); !state.Recovered {
		t.Fatalf("adoption = %#v", state)
	}
	mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, f.base, f.preserved)

	replay := f.service.AdoptTerminalHead(f.ctx, request)
	if replay.Recovered || replay.Changed || replay.Safety != "blocked_adopt_terminal_mirror_mismatch" {
		t.Fatalf("replay against a moved lane = %#v", replay)
	}
	if got := f.mirrorLane(); got != f.base {
		t.Fatalf("replay moved the independently moved lane to %s", got)
	}
	if strings.Contains(replay.Error, "custody was not returned") || !strings.Contains(replay.Error, "custody was already recorded") {
		t.Fatalf("replay refusal misreports custody: %q", replay.Error)
	}
	if !f.custodyReturned() {
		t.Fatal("replay refusal cleared the recorded custody return")
	}
}

func TestAdoptTerminalHeadMirrorLaneShapes(t *testing.T) {
	t.Parallel()

	t.Run("lane already at preserved head", func(t *testing.T) {
		f := newAuthorizedRewriteRecoverFixture(t)
		if state := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)); !state.Recovered || !state.Changed {
			t.Fatalf("adoption = %#v", state)
		}
		if got := f.mirrorLane(); got != f.preserved {
			t.Fatalf("lane = %s, want %s", got, f.preserved)
		}
		if got := readGitOptional(t, f.gate, f.localAnchorRef()); got != "" {
			t.Fatalf("an unmoved lane needs no gate caller anchor, got %s", got)
		}
		reconciled, err := gate.ReconcileStaleBranch(f.ctx, f.gate, f.local, f.run.Branch, f.preserved, "")
		if err != nil || reconciled.Reconciled {
			t.Fatalf("next-run preparation = %#v, err %v", reconciled, err)
		}
	})

	t.Run("lane absent", func(t *testing.T) {
		f := newStaleMirrorAdoptionFixture(t)
		mustRun(t, f.gate, "update-ref", "-d", "refs/heads/"+f.run.Branch, f.submitted)
		if state := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)); !state.Recovered || !state.Changed {
			t.Fatalf("adoption = %#v", state)
		}
		if got := f.mirrorLane(); got != "" {
			t.Fatalf("adoption created an absent lane at %s", got)
		}
		if status := f.service.InspectCached(f.ctx); status.Safety != "custody_returned" || status.NextAction == nil || status.NextAction.Code != "run_pipeline" {
			t.Fatalf("status after an absent-lane adoption = %#v", status)
		}
		reconciled, err := gate.ReconcileStaleBranch(f.ctx, f.gate, f.local, f.run.Branch, f.preserved, "")
		if err != nil || reconciled.Reconciled {
			t.Fatalf("next-run preparation = %#v, err %v", reconciled, err)
		}
	})
}

func TestAdoptTerminalHeadRefusesMismatchedMirrorBindingsWithoutMutation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantSafety string
		wantLane   func(*recoverFixture) string
		mutate     func(*testing.T, *recoverFixture)
	}{
		{name: "lane moved to unrelated commit", wantSafety: "blocked_adopt_terminal_mirror_mismatch", wantLane: func(f *recoverFixture) string { return f.base },
			mutate: func(t *testing.T, f *recoverFixture) {
				mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, f.base, f.submitted)
			}},
		{name: "lane symbolic", wantSafety: "blocked_adopt_terminal_mirror_mismatch", wantLane: func(f *recoverFixture) string { return f.submitted },
			mutate: func(t *testing.T, f *recoverFixture) {
				mustRun(t, f.gate, "update-ref", "-d", "refs/heads/"+f.run.Branch, f.submitted)
				mustRun(t, f.gate, "update-ref", "refs/heads/elsewhere", f.submitted)
				mustRun(t, f.gate, "symbolic-ref", "refs/heads/"+f.run.Branch, "refs/heads/elsewhere")
			}},
		{name: "conflicting gate caller anchor", wantSafety: "blocked_adopt_terminal_mirror_mismatch", wantLane: func(f *recoverFixture) string { return f.submitted },
			mutate: func(t *testing.T, f *recoverFixture) {
				mustRun(t, f.gate, "update-ref", f.localAnchorRef(), f.base)
			}},
		{name: "symbolic gate caller anchor", wantSafety: "blocked_adopt_terminal_mirror_mismatch", wantLane: func(f *recoverFixture) string { return f.submitted },
			mutate: func(t *testing.T, f *recoverFixture) {
				mustRun(t, f.gate, "symbolic-ref", f.localAnchorRef(), "refs/heads/"+f.run.Branch)
			}},
		{name: "dirty tracked work", wantSafety: "blocked_adopt_terminal_dirty", wantLane: func(f *recoverFixture) string { return f.submitted },
			mutate: func(t *testing.T, f *recoverFixture) {
				mustWrite(t, filepath.Join(f.local, "feature.txt"), "dirty tracked work\n")
			}},
		{name: "unique local work", wantSafety: "blocked_adopt_terminal_caller_changed", wantLane: func(f *recoverFixture) string { return f.submitted },
			mutate: func(t *testing.T, f *recoverFixture) {
				mustWrite(t, filepath.Join(f.local, "unique.txt"), "unique\n")
				mustRun(t, f.local, "add", "unique.txt")
				mustRun(t, f.local, "commit", "-m", "unique local work")
			}},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newStaleMirrorAdoptionFixture(t)
			tc.mutate(t, f)
			headBefore := mustRun(t, f.local, "rev-parse", "HEAD")
			statusBefore := mustRun(t, f.local, "status", "--porcelain=v1", "--untracked-files=all")
			anchorBefore := readGitOptional(t, f.gate, f.localAnchorRef())

			state := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f))
			if state.Recovered || state.Changed || state.Safety != tc.wantSafety {
				t.Fatalf("refusal = %#v, want %s", state, tc.wantSafety)
			}
			if got := mustRun(t, f.local, "rev-parse", "HEAD"); got != headBefore {
				t.Fatalf("refusal moved HEAD to %s", got)
			}
			if got := mustRun(t, f.local, "status", "--porcelain=v1", "--untracked-files=all"); got != statusBefore {
				t.Fatalf("refusal changed local work: %q -> %q", statusBefore, got)
			}
			if got := readGitOptional(t, f.gate, "refs/heads/"+f.run.Branch); got != tc.wantLane(f) {
				t.Fatalf("refusal moved the mirror lane to %s", got)
			}
			if got := readGitOptional(t, f.gate, f.localAnchorRef()); got != anchorBefore {
				t.Fatalf("refusal changed the gate caller anchor from %q to %q", anchorBefore, got)
			}
			run, err := f.db.GetRun(f.run.ID)
			if err != nil || run.CustodyReturnedAt != nil || run.TerminalAdoptionAuthorizedAt != nil {
				t.Fatalf("refusal changed the adoption record: %#v err %v", run, err)
			}
		})
	}
}

func TestAdoptTerminalHeadMirrorInterruptionsRemainRetryable(t *testing.T) {
	t.Parallel()

	assertInterrupted := func(t *testing.T, f *recoverFixture, wantLane string) {
		t.Helper()
		if f.custodyReturned() {
			t.Fatal("interrupted mirror settlement stamped custody")
		}
		if got := f.mirrorLane(); got != wantLane {
			t.Fatalf("interrupted lane = %s, want %s", got, wantLane)
		}
		if got := mustRun(t, f.gate, "rev-parse", f.localAnchorRef()); got != f.submitted {
			t.Fatalf("interrupted gate caller anchor = %s", got)
		}
		if got := mustRun(t, f.local, "rev-parse", f.localAnchorRef()); got != f.submitted {
			t.Fatalf("interrupted worktree caller anchor = %s", got)
		}
		if got := mustRun(t, f.gate, "rev-parse", f.anchorRef()); got != f.preserved {
			t.Fatalf("interrupted preserved anchor = %s", got)
		}
	}
	retry := func(t *testing.T, f *recoverFixture) {
		t.Helper()
		f.service.afterTerminalAdoptionMirrorAnchor = nil
		f.service.afterTerminalAdoptionMirrorMove = nil
		f.service.completeTerminalAdoption = nil
		retried := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f))
		if !retried.Recovered || retried.Changed {
			t.Fatalf("retry after mirror interruption = %#v", retried)
		}
		assertRecoveredRewriteFixture(t, f)
		if replay := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)); !replay.Recovered || replay.Changed {
			t.Fatalf("replay after retry = %#v", replay)
		}
		assertNextRunMirrorReady(t, f)
	}
	crash := func(t *testing.T, run func()) {
		t.Helper()
		crashed := false
		func() {
			defer func() { crashed = recover() != nil }()
			run()
		}()
		if !crashed {
			t.Fatal("simulated interruption did not stop the adoption")
		}
	}

	t.Run("after gate caller anchor", func(t *testing.T) {
		t.Parallel()
		f := newStaleMirrorAdoptionFixture(t)
		f.service.afterTerminalAdoptionMirrorAnchor = func() { panic("simulated stop after gate anchor") }
		crash(t, func() { _ = f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)) })
		assertInterrupted(t, f, f.submitted)
		retry(t, f)
	})

	t.Run("after lane compare and swap", func(t *testing.T) {
		t.Parallel()
		f := newStaleMirrorAdoptionFixture(t)
		f.service.afterTerminalAdoptionMirrorMove = func() { panic("simulated stop after lane CAS") }
		crash(t, func() { _ = f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)) })
		assertInterrupted(t, f, f.preserved)
		retry(t, f)
	})

	t.Run("custody stamp fails after settlement", func(t *testing.T) {
		t.Parallel()
		f := newStaleMirrorAdoptionFixture(t)
		f.service.completeTerminalAdoption = func(db.TerminalHeadAdoptionAuthorization) (bool, error) {
			return false, errors.New("simulated interruption")
		}
		state := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f))
		if state.Recovered || state.Safety != "blocked_adopt_terminal_stamp_failed" {
			t.Fatalf("stamp interruption = %#v", state)
		}
		assertInterrupted(t, f, f.preserved)
		retry(t, f)
	})

	// Earlier boundaries are owned by TestAdoptTerminalHeadInterruptedAttemptsRemainRetryable;
	// here they only need to prove the resumed adoption still settles the lane.
	t.Run("after branch compare and swap", func(t *testing.T) {
		t.Parallel()
		f := newStaleMirrorAdoptionFixture(t)
		f.service.afterRecoverBranchMove = func() { panic("simulated stop after branch CAS") }
		crash(t, func() { _ = f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)) })
		if got := f.mirrorLane(); got != f.submitted {
			t.Fatalf("lane moved before the worktree move completed: %s", got)
		}
		f.service.afterRecoverBranchMove = nil
		if retried := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)); !retried.Recovered || !retried.Changed {
			t.Fatalf("retry = %#v", retried)
		}
		assertRecoveredRewriteFixture(t, f)
		assertNextRunMirrorReady(t, f)
	})

	t.Run("after detaching caller snapshot", func(t *testing.T) {
		t.Parallel()
		f := newStaleMirrorAdoptionFixture(t)
		f.service.afterRecoverBranchMove = func() {
			mustRun(t, f.local, "checkout", "--no-overwrite-ignore", "--detach", f.submitted)
			panic("simulated stop after detach")
		}
		crash(t, func() { _ = f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)) })
		f.service.afterRecoverBranchMove = nil
		if retried := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)); !retried.Recovered || !retried.Changed {
			t.Fatalf("retry = %#v", retried)
		}
		assertRecoveredRewriteFixture(t, f)
		assertNextRunMirrorReady(t, f)
	})
}

func TestAdoptTerminalHeadMirrorRacesNeverOverwriteTheLane(t *testing.T) {
	t.Parallel()

	t.Run("concurrent push moves the lane", func(t *testing.T) {
		t.Parallel()
		f := newStaleMirrorAdoptionFixture(t)
		f.service.afterTerminalAdoptionMirrorAnchor = func() {
			mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, f.base, f.submitted)
		}
		state := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f))
		if state.Recovered || state.Safety != "blocked_adopt_terminal_mirror_race" {
			t.Fatalf("lane race = %#v", state)
		}
		if got := f.mirrorLane(); got != f.base {
			t.Fatalf("race overwrote the concurrent lane head: %s", got)
		}
		if f.custodyReturned() {
			t.Fatal("lane race stamped custody")
		}
		if got := mustRun(t, f.gate, "rev-parse", f.localAnchorRef()); got != f.submitted {
			t.Fatalf("race lost the caller anchor: %s", got)
		}

		f.service.afterTerminalAdoptionMirrorAnchor = nil
		if refused := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)); refused.Recovered || refused.Safety != "blocked_adopt_terminal_mirror_mismatch" {
			t.Fatalf("retry against an independently moved lane = %#v", refused)
		}
		if got := f.mirrorLane(); got != f.base {
			t.Fatalf("refused retry moved the lane: %s", got)
		}
		mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, f.submitted, f.base)
		if retried := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)); !retried.Recovered {
			t.Fatalf("retry after restoring the exact lane = %#v", retried)
		}
		assertNextRunMirrorReady(t, f)
	})

	t.Run("concurrent identical settlement", func(t *testing.T) {
		t.Parallel()
		f := newStaleMirrorAdoptionFixture(t)
		f.service.afterTerminalAdoptionMirrorAnchor = func() {
			mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, f.preserved, f.submitted)
		}
		if state := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)); !state.Recovered || !state.Changed {
			t.Fatalf("adoption racing an identical settlement = %#v", state)
		}
		assertNextRunMirrorReady(t, f)
	})

	t.Run("preserved anchor moves before the lane CAS", func(t *testing.T) {
		t.Parallel()
		f := newStaleMirrorAdoptionFixture(t)
		f.service.afterTerminalAdoptionMirrorAnchor = func() {
			mustRun(t, f.gate, "update-ref", f.anchorRef(), f.base, f.preserved)
		}
		state := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f))
		if state.Recovered || state.Safety != "blocked_adopt_terminal_mirror_mismatch" {
			t.Fatalf("anchor race = %#v", state)
		}
		if got := f.mirrorLane(); got != f.submitted {
			t.Fatalf("anchor race moved the lane: %s", got)
		}
		if f.custodyReturned() {
			t.Fatal("anchor race stamped custody")
		}
	})
}

// TestAdoptTerminalHeadStatusOffersNothingForAMovedMirrorLane keeps status and
// the mutating action on one predicate: a lane adoption would refuse to settle
// is never offered as adopt_terminal_head.
func TestAdoptTerminalHeadStatusOffersNothingForAMovedMirrorLane(t *testing.T) {
	t.Parallel()

	f := newStaleMirrorAdoptionFixture(t)
	offered := f.service.InspectCached(f.ctx)
	if offered.NextAction == nil || offered.NextAction.Code != "adopt_terminal_head" {
		t.Fatalf("stale-lane status should offer adoption: %#v", offered)
	}
	mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, f.base, f.submitted)
	moved := f.service.InspectCached(f.ctx)
	if moved.NextAction != nil && moved.NextAction.Code == "adopt_terminal_head" {
		t.Fatalf("status offered adoption for a lane it would refuse: %#v", moved)
	}
	if f.run.Status != types.RunFailed {
		t.Fatalf("fixture status = %s", f.run.Status)
	}
}

// TestAdoptTerminalHeadNeverTreatsAnUnreadableLaneAsAbsent covers a lane that
// becomes symbolic while adoption is settling it. Its read errors, and an
// errored read must refuse rather than pass as an absent lane that needs no
// move and lets custody be stamped.
func TestAdoptTerminalHeadNeverTreatsAnUnreadableLaneAsAbsent(t *testing.T) {
	t.Parallel()

	makeLaneSymbolic := func(t *testing.T, f *recoverFixture, from string) {
		t.Helper()
		mustRun(t, f.gate, "update-ref", "refs/heads/elsewhere", from)
		mustRun(t, f.gate, "update-ref", "-d", "refs/heads/"+f.run.Branch, from)
		mustRun(t, f.gate, "symbolic-ref", "refs/heads/"+f.run.Branch, "refs/heads/elsewhere")
	}
	restoreLane := func(t *testing.T, f *recoverFixture, to string) {
		t.Helper()
		mustRun(t, f.gate, "symbolic-ref", "--delete", "refs/heads/"+f.run.Branch)
		mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, to, "")
	}

	tests := []struct {
		name       string
		wantSafety string
		arm        func(*testing.T, *recoverFixture)
		restoreTo  func(*recoverFixture) string
	}{
		{name: "symbolic before settlement", wantSafety: "blocked_adopt_terminal_mirror_mismatch",
			arm: func(t *testing.T, f *recoverFixture) {
				f.service.afterRecoverBranchMove = func() { makeLaneSymbolic(t, f, f.submitted) }
			},
			restoreTo: func(f *recoverFixture) string { return f.submitted }},
		{name: "symbolic after the lane move", wantSafety: "blocked_adopt_terminal_mirror_race",
			arm: func(t *testing.T, f *recoverFixture) {
				f.service.afterTerminalAdoptionMirrorMove = func() { makeLaneSymbolic(t, f, f.preserved) }
			},
			restoreTo: func(f *recoverFixture) string { return f.preserved }},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newStaleMirrorAdoptionFixture(t)
			tc.arm(t, f)
			state := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f))
			if state.Recovered || state.Safety != tc.wantSafety {
				t.Fatalf("unreadable lane at settlement = %#v, want %s", state, tc.wantSafety)
			}
			if f.custodyReturned() {
				t.Fatal("an unreadable lane stamped custody")
			}
			if target := mustRun(t, f.gate, "symbolic-ref", "refs/heads/"+f.run.Branch); target != "refs/heads/elsewhere" {
				t.Fatalf("refusal rewrote the symbolic lane to %s", target)
			}

			f.service.afterRecoverBranchMove = nil
			f.service.afterTerminalAdoptionMirrorMove = nil
			restoreLane(t, f, tc.restoreTo(f))
			if retried := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)); !retried.Recovered {
				t.Fatalf("retry after restoring the direct lane = %#v", retried)
			}
			assertRecoveredRewriteFixture(t, f)
			assertNextRunMirrorReady(t, f)
		})
	}
}

// TestAdoptTerminalHeadReverifiesTheCallerAfterMirrorSettlement changes the
// caller or a history anchor after the lane has moved but before custody is
// stamped. The stamp checks only run fields, so the caller verification must
// run again at that boundary: every change refuses without stamping custody or
// touching operator content, and the same bound action succeeds once the exact
// state is restored.
func TestAdoptTerminalHeadReverifiesTheCallerAfterMirrorSettlement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		change  func(*testing.T, *recoverFixture)
		verify  func(*testing.T, *recoverFixture)
		restore func(*testing.T, *recoverFixture)
	}{
		{name: "dirty caller edit",
			change: func(t *testing.T, f *recoverFixture) {
				mustWrite(t, filepath.Join(f.local, "feature.txt"), "operator edit during settlement\n")
			},
			verify: func(t *testing.T, f *recoverFixture) {
				if got := mustRun(t, f.local, "status", "--porcelain=v1"); !strings.Contains(got, "feature.txt") {
					t.Fatalf("refusal lost the operator edit: %q", got)
				}
			},
			restore: func(t *testing.T, f *recoverFixture) { mustRun(t, f.local, "checkout", "--", "feature.txt") }},
		{name: "caller branch switch",
			change: func(t *testing.T, f *recoverFixture) {
				mustRun(t, f.local, "checkout", "-b", "operator-side", f.preserved)
			},
			verify: func(t *testing.T, f *recoverFixture) {
				if got := mustRun(t, f.local, "symbolic-ref", "--short", "HEAD"); got != "operator-side" {
					t.Fatalf("refusal switched the operator's branch back to %s", got)
				}
			},
			restore: func(t *testing.T, f *recoverFixture) { mustRun(t, f.local, "checkout", f.run.Branch) }},
		{name: "second worktree attaches the branch",
			change: func(t *testing.T, f *recoverFixture) {
				mustRun(t, f.local, "worktree", "add", "--force", filepath.Join(t.TempDir(), "second"), f.run.Branch)
			},
			restore: func(t *testing.T, f *recoverFixture) {
				for _, line := range strings.Split(mustRun(t, f.local, "worktree", "list", "--porcelain"), "\n") {
					if path, ok := strings.CutPrefix(line, "worktree "); ok && strings.HasSuffix(path, "second") {
						mustRun(t, f.local, "worktree", "remove", "--force", path)
					}
				}
			}},
		{name: "gate preserved anchor moves",
			change: func(t *testing.T, f *recoverFixture) {
				mustRun(t, f.gate, "update-ref", f.anchorRef(), f.base, f.preserved)
			},
			restore: func(t *testing.T, f *recoverFixture) {
				mustRun(t, f.gate, "update-ref", f.anchorRef(), f.preserved, f.base)
			}},
		{name: "worktree preserved anchor moves",
			change: func(t *testing.T, f *recoverFixture) {
				mustRun(t, f.local, "update-ref", f.anchorRef(), f.base, f.preserved)
			},
			restore: func(t *testing.T, f *recoverFixture) {
				mustRun(t, f.local, "update-ref", f.anchorRef(), f.preserved, f.base)
			}},
		{name: "worktree caller anchor moves",
			change: func(t *testing.T, f *recoverFixture) {
				mustRun(t, f.local, "update-ref", f.localAnchorRef(), f.base, f.submitted)
			},
			restore: func(t *testing.T, f *recoverFixture) {
				mustRun(t, f.local, "update-ref", f.localAnchorRef(), f.submitted, f.base)
			}},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newStaleMirrorAdoptionFixture(t)
			f.service.afterTerminalAdoptionMirrorMove = func() { tc.change(t, f) }
			state := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f))
			if state.Recovered || state.Safety != "blocked_adopt_terminal_incomplete" {
				t.Fatalf("change during settlement = %#v", state)
			}
			if strings.Contains(state.Error, "custody was returned") || !strings.Contains(state.Error, "custody was not returned") {
				t.Fatalf("refusal misreports custody: %q", state.Error)
			}
			if f.custodyReturned() {
				t.Fatal("a change during settlement stamped custody")
			}
			if got := mustRun(t, f.gate, "rev-parse", f.localAnchorRef()); got != f.submitted {
				t.Fatalf("gate caller anchor = %s, want submitted %s", got, f.submitted)
			}
			if tc.verify != nil {
				tc.verify(t, f)
			}

			f.service.afterTerminalAdoptionMirrorMove = nil
			tc.restore(t, f)
			if retried := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)); !retried.Recovered {
				t.Fatalf("retry after restoring the exact state = %#v", retried)
			}
			assertRecoveredRewriteFixture(t, f)
			assertNextRunMirrorReady(t, f)
		})
	}
}

func TestAdoptTerminalHeadInterruptedStatusRoutesBoundReplay(t *testing.T) {
	t.Parallel()
	for _, throughRecover := range []bool{false, true} {
		t.Run(fmt.Sprint("recover=", throughRecover), func(t *testing.T) {
			t.Parallel()
			f := newStaleMirrorAdoptionFixture(t)
			request := terminalHeadAdoptionRequest(f)
			f.service.afterTerminalAdoptionMirrorAnchor = func() { panic("interrupted settlement") }
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("expected interruption")
					}
				}()
				f.service.AdoptTerminalHead(f.ctx, request)
			}()
			f.service.afterTerminalAdoptionMirrorAnchor = nil
			status := f.service.InspectCached(f.ctx)
			if status.NextAction == nil || status.NextAction.Code != "adopt_terminal_head" || status.NextAction.Command != terminalHeadAdoptionCommand(request) || f.custodyReturned() ||
				status.Safety != "blocked_terminal_head_adoption_replay_required" || status.Recovery == nil || status.Recovery.Proof != "operator_authorized" {
				t.Fatalf("interrupted recovery offer = %#v", status)
			}
			if kept := f.service.Recover(f.ctx, true); kept.Recovered || kept.Safety != "blocked_adopt_terminal_keep_local" || f.custodyReturned() || f.mirrorLane() != f.submitted ||
				kept.NextAction == nil || kept.NextAction.Code != "adopt_terminal_head" || kept.NextAction.Command != terminalHeadAdoptionCommand(request) || kept.Recovery == nil || kept.Recovery.Proof != "operator_authorized" {
				t.Fatalf("keep-local changed the recorded adoption: %#v", kept)
			}
			var result State
			if throughRecover {
				result = f.service.Recover(f.ctx, false)
			} else {
				result = f.service.AdoptTerminalHead(f.ctx, request)
			}
			if !result.Recovered || result.Changed {
				t.Fatalf("guided replay = %#v", result)
			}
			assertNextRunMirrorReady(t, f)
		})
	}
}

func TestAdoptTerminalHeadCompletedStatusOffersOnlyExactStaleLaneReplay(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"exact", "later commit", "dirty", "gate anchor moved", "caller anchor moved", "lane moved"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			f := newStaleMirrorAdoptionFixture(t)
			request := terminalHeadAdoptionRequest(f)
			if result := f.service.AdoptTerminalHead(f.ctx, request); !result.Recovered {
				t.Fatalf("adoption = %#v", result)
			}
			mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, f.submitted, f.preserved)
			switch shape {
			case "later commit":
				mustRun(t, f.local, "commit", "--allow-empty", "-m", "later work")
			case "dirty":
				mustWrite(t, filepath.Join(f.local, "later.txt"), "uncommitted work\n")
			case "gate anchor moved":
				mustRun(t, f.gate, "update-ref", f.anchorRef(), f.base, f.preserved)
			case "caller anchor moved":
				mustRun(t, f.local, "update-ref", f.localAnchorRef(), f.base, f.submitted)
			case "lane moved":
				mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, f.base, f.submitted)
			}
			status := f.service.InspectCached(f.ctx)
			offered := status.NextAction != nil && status.NextAction.Code == "adopt_terminal_head"
			if offered != (shape == "exact") {
				t.Fatalf("%s replay offer = %#v", shape, status)
			}
			if TerminalAdoptionUnsettled(status) != (shape != "lane moved") {
				t.Fatalf("%s unsettled classification = %#v", shape, status)
			}
			if shape != "exact" && shape != "lane moved" && (status.Safety != "blocked_adopt_terminal_replay_mismatch" ||
				status.NextAction == nil || status.NextAction.Code != "inspect_and_reconcile_manually" || !strings.Contains(status.Error, f.preserved)) {
				t.Fatalf("%s stale-lane mismatch = %#v", shape, status)
			}
			if shape == "exact" {
				if status.NextAction.Command != terminalHeadAdoptionCommand(request) {
					t.Fatal("bindings changed")
				}
				if result := f.service.AdoptTerminalHead(f.ctx, request); !result.Recovered {
					t.Fatalf("offered replay = %#v", result)
				}
				assertNextRunMirrorReady(t, f)
			}
		})
	}
}

// TestRecoverAfterSettledAdoptionStaysAnIdempotentNoop keeps a completed
// adoption whose lane is already settled (or absent) on the ordinary
// custody-returned no-op for plain and keep-local recovery, even after later
// operator work, while a still-stale caller-head lane keeps routing through
// the bound replay and refuses rather than reporting readiness.
func TestRecoverAfterSettledAdoptionStaysAnIdempotentNoop(t *testing.T) {
	t.Parallel()
	for _, lane := range []string{"settled", "absent", "stale"} {
		for _, work := range []string{"later commit", "dirty"} {
			for _, keepLocal := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/keep-local=%v", lane, work, keepLocal), func(t *testing.T) {
					t.Parallel()
					f := newStaleMirrorAdoptionFixture(t)
					if result := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f)); !result.Recovered {
						t.Fatalf("adoption = %#v", result)
					}
					switch lane {
					case "absent":
						mustRun(t, f.gate, "update-ref", "-d", "refs/heads/"+f.run.Branch, f.preserved)
					case "stale":
						mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, f.submitted, f.preserved)
					}
					switch work {
					case "later commit":
						mustRun(t, f.local, "commit", "--allow-empty", "-m", "later work")
					case "dirty":
						mustWrite(t, filepath.Join(f.local, "later.txt"), "uncommitted work\n")
					}
					head := mustRun(t, f.local, "rev-parse", "HEAD")
					laneBefore := f.mirrorLane()

					result := f.service.Recover(f.ctx, keepLocal)
					if lane == "stale" {
						want := "blocked_adopt_terminal_replay_mismatch"
						if keepLocal {
							want = "blocked_adopt_terminal_keep_local"
						}
						if result.Recovered || result.Safety != want || !f.custodyReturned() || !TerminalAdoptionUnsettled(result) {
							t.Fatalf("stale lane recovery = %#v, want refusal %s with custody kept", result, want)
						}
					} else if !result.Recovered || result.Changed {
						t.Fatalf("settled adoption recovery = %#v", result)
					}
					if got := mustRun(t, f.local, "rev-parse", "HEAD"); got != head {
						t.Fatalf("recovery moved HEAD to %s, want %s", got, head)
					}
					if got := f.mirrorLane(); got != laneBefore {
						t.Fatalf("recovery moved the lane to %q, want %q", got, laneBefore)
					}
					if work == "dirty" {
						if status := mustRun(t, f.local, "status", "--porcelain"); !strings.Contains(status, "later.txt") {
							t.Fatalf("recovery discarded uncommitted work: %q", status)
						}
					}
				})
			}
		}
	}
}

// TestAdoptTerminalHeadReplayWritesNoGateAnchorOnMissingPreservedEvidence
// keeps the completed-adoption replay fail-closed without Git mutation: a
// missing gate preserved-head anchor refuses before the caller-head anchor is
// written or the lane moves.
func TestAdoptTerminalHeadReplayWritesNoGateAnchorOnMissingPreservedEvidence(t *testing.T) {
	t.Parallel()
	f := newStaleMirrorAdoptionFixture(t)
	request := terminalHeadAdoptionRequest(f)
	if result := f.service.AdoptTerminalHead(f.ctx, request); !result.Recovered {
		t.Fatalf("adoption = %#v", result)
	}
	mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, f.submitted, f.preserved)
	mustRun(t, f.gate, "update-ref", "-d", f.localAnchorRef(), f.submitted)
	mustRun(t, f.gate, "update-ref", "-d", f.anchorRef(), f.preserved)

	result := f.service.AdoptTerminalHead(f.ctx, request)
	if result.Recovered || result.Safety != "blocked_adopt_terminal_mirror_mismatch" {
		t.Fatalf("replay with missing gate preserved evidence = %#v", result)
	}
	if got := readGitOptional(t, f.gate, f.localAnchorRef()); got != "" {
		t.Fatalf("refused replay wrote gate caller anchor at %s", got)
	}
	if got := f.mirrorLane(); got != f.submitted {
		t.Fatalf("refused replay moved the lane to %s", got)
	}
}

// TestAdoptTerminalHeadAuthorizedCallerChangeNamesTheBoundHeads keeps the
// refusal for an authorized adoption whose caller later committed nondestructive
// and honest: nothing moves, custody is not returned, and the diagnostic names
// the exact heads the recorded authorization can still continue from.
func TestAdoptTerminalHeadAuthorizedCallerChangeNamesTheBoundHeads(t *testing.T) {
	t.Parallel()
	f := newStaleMirrorAdoptionFixture(t)
	request := terminalHeadAdoptionRequest(f)
	f.service.afterTerminalAdoptionAuthorized = func() { panic("interrupted after authorization") }
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected interruption")
			}
		}()
		f.service.AdoptTerminalHead(f.ctx, request)
	}()
	f.service.afterTerminalAdoptionAuthorized = nil
	mustRun(t, f.local, "commit", "--allow-empty", "-m", "later work")
	later := mustRun(t, f.local, "rev-parse", "HEAD")

	for _, result := range []State{f.service.AdoptTerminalHead(f.ctx, request), f.service.Recover(f.ctx, false)} {
		if result.Recovered || result.Safety != "blocked_adopt_terminal_caller_changed" || f.custodyReturned() ||
			!strings.Contains(result.Error, f.submitted) || !strings.Contains(result.Error, f.preserved) {
			t.Fatalf("authorized caller change = %#v", result)
		}
	}
	if got := mustRun(t, f.local, "rev-parse", "HEAD"); got != later {
		t.Fatalf("refusal moved HEAD to %s", got)
	}
	if got := f.mirrorLane(); got != f.submitted {
		t.Fatalf("refusal moved the lane to %s", got)
	}
}

// TestAdoptTerminalHeadInterruptedThenCommittedAdvertisesNoRefusingAction
// keeps status honest after an authorized adoption was interrupted past its
// branch move and the operator then committed: the only executable continuation
// is from an exact bound head, so status advertises manual reconciliation
// naming both heads instead of an ordinary recovery Recover would refuse.
func TestAdoptTerminalHeadInterruptedThenCommittedAdvertisesNoRefusingAction(t *testing.T) {
	t.Parallel()
	f := newStaleMirrorAdoptionFixture(t)
	request := terminalHeadAdoptionRequest(f)
	f.service.afterTerminalAdoptionMirrorAnchor = func() { panic("interrupted settlement") }
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected interruption")
			}
		}()
		f.service.AdoptTerminalHead(f.ctx, request)
	}()
	f.service.afterTerminalAdoptionMirrorAnchor = nil
	mustRun(t, f.local, "commit", "--allow-empty", "-m", "later work")

	status := f.service.InspectCached(f.ctx)
	if status.State != StatePipelineOwned || status.Safety != "blocked_recover_manual_reconciliation" || status.NextAction == nil || status.NextAction.Code != "inspect_and_reconcile_manually" ||
		!strings.Contains(status.Error, f.submitted) || !strings.Contains(status.Error, f.preserved) {
		t.Fatalf("status for an interrupted adoption with later work = %#v", status)
	}
	if result := f.service.Recover(f.ctx, false); result.Recovered || result.Safety != "blocked_adopt_terminal_caller_changed" || f.custodyReturned() {
		t.Fatalf("recover after later work = %#v", result)
	}
}

// TestAdoptTerminalHeadInterruptedAtPreservedHeadWithMovedLaneAdvertisesNoRefusingAction
// covers an adoption interrupted after the branch reached the preserved head
// whose private lane then moved to an unrelated commit: the bound action
// refuses there, so status must report manual reconciliation instead of an
// ordinary recovery, and nothing may move.
func TestAdoptTerminalHeadInterruptedAtPreservedHeadWithMovedLaneAdvertisesNoRefusingAction(t *testing.T) {
	t.Parallel()
	f := newStaleMirrorAdoptionFixture(t)
	request := terminalHeadAdoptionRequest(f)
	f.service.afterTerminalAdoptionMirrorAnchor = func() { panic("interrupted settlement") }
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected interruption")
			}
		}()
		f.service.AdoptTerminalHead(f.ctx, request)
	}()
	f.service.afterTerminalAdoptionMirrorAnchor = nil
	mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, f.base, f.submitted)

	status := f.service.InspectCached(f.ctx)
	if status.State != StatePipelineOwned || status.Safety != "blocked_recover_manual_reconciliation" ||
		status.NextAction == nil || status.NextAction.Code != "inspect_and_reconcile_manually" ||
		status.Recovery == nil || status.Recovery.Source != "terminal_head_adoption" ||
		!strings.Contains(status.Error, f.preserved) || !strings.Contains(status.Error, "exact interrupted caller snapshot") {
		t.Fatalf("status for an interrupted adoption with a moved lane = %#v", status)
	}
	if result := f.service.Recover(f.ctx, false); result.Recovered || f.custodyReturned() {
		t.Fatalf("recover with a moved lane = %#v", result)
	}
	if got := f.mirrorLane(); got != f.base {
		t.Fatalf("refusal moved the lane to %s", got)
	}
	if got := mustRun(t, f.local, "rev-parse", "HEAD"); got != f.preserved {
		t.Fatalf("refusal moved HEAD to %s", got)
	}
}

func TestAdoptTerminalHeadCallerAnchorDisappearsAfterMirrorMove(t *testing.T) {
	t.Parallel()
	f := newStaleMirrorAdoptionFixture(t)
	f.service.afterTerminalAdoptionMirrorMove = func() { mustRun(t, f.gate, "update-ref", "-d", f.localAnchorRef(), f.submitted) }
	result := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f))
	if result.Recovered || result.Safety != "blocked_adopt_terminal_mirror_race" || f.custodyReturned() {
		t.Fatalf("anchor race = %#v", result)
	}
	if got := f.mirrorLane(); got != f.preserved {
		t.Fatalf("lane = %s", got)
	}
}

func TestAdoptTerminalHeadLaneMovesAfterSettlement(t *testing.T) {
	t.Parallel()
	f := newStaleMirrorAdoptionFixture(t)
	f.service.afterTerminalAdoptionSettlement = func() { mustRun(t, f.gate, "update-ref", "refs/heads/"+f.run.Branch, f.base, f.preserved) }
	result := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f))
	if result.Recovered || result.Safety != "blocked_adopt_terminal_incomplete" || f.custodyReturned() {
		t.Fatalf("post-settlement lane race = %#v", result)
	}
	if got := f.mirrorLane(); got != f.base {
		t.Fatalf("concurrent lane overwritten: %s", got)
	}
}

func TestAdoptTerminalHeadSiblingRunStartsAtStamp(t *testing.T) {
	t.Parallel()
	f := newStaleMirrorAdoptionFixture(t)
	f.service.completeTerminalAdoption = func(auth db.TerminalHeadAdoptionAuthorization) (bool, error) {
		sibling, err := f.db.InsertRun(f.repo.ID, f.run.Branch, f.preserved, f.base)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.db.UpdateRunStatus(sibling.ID, types.RunRunning); err != nil {
			t.Fatal(err)
		}
		return f.db.CompleteTerminalHeadAdoption(auth)
	}
	result := f.service.AdoptTerminalHead(f.ctx, terminalHeadAdoptionRequest(f))
	if result.Recovered || result.Safety != "blocked_adopt_terminal_stamp_failed" || f.custodyReturned() {
		t.Fatalf("active sibling at stamp = %#v", result)
	}
}

func TestAdoptTerminalHeadResumeRefusesActiveSibling(t *testing.T) {
	t.Parallel()
	f := newStaleMirrorAdoptionFixture(t)
	request := terminalHeadAdoptionRequest(f)
	f.service.afterRecoverBranchMove = func() { panic("interrupted branch move") }
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected interruption")
			}
		}()
		f.service.AdoptTerminalHead(f.ctx, request)
	}()
	f.service.afterRecoverBranchMove = nil
	sibling, err := f.db.InsertRun(f.repo.ID, f.run.Branch, f.preserved, f.base)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.db.UpdateRunStatus(sibling.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	before := mustRun(t, f.local, "status", "--porcelain=v1")
	result := f.service.AdoptTerminalHead(f.ctx, request)
	if result.Recovered || result.Safety != "blocked_adopt_terminal_assumptions_changed" || f.custodyReturned() {
		t.Fatalf("resume with active sibling = %#v", result)
	}
	if got := mustRun(t, f.local, "status", "--porcelain=v1"); got != before {
		t.Fatalf("resume changed caller snapshot: %q -> %q", before, got)
	}
}

func TestTerminalHeadAdoptionCommandRoundTripsAllBindings(t *testing.T) {
	t.Parallel()
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("POSIX shell unavailable")
	}
	request := TerminalHeadAdoptionRequest{
		RepositoryID: "repo'$(printf repo);\"",
		Branch:       "feat/it's;$(printf branch)`printf tick`&|<>$HOME",
		RunID:        "run'\";$(printf run)", CallerHead: "caller'$(printf caller)", PreservedHead: "preserved'`printf preserved`",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, sh, "-c", "set -- "+terminalHeadAdoptionCommand(request)+"; printf '%s\\0' \"$@\"")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("argv echo: %v: %s", err, output)
	}
	want := []string{"no-mistakes", "axi", "sync", "--adopt-terminal-head", "--repository", request.RepositoryID, "--branch", request.Branch, "--terminal-run", request.RunID, "--caller-head", request.CallerHead, "--preserved-head", request.PreservedHead}
	if got := strings.Split(strings.TrimSuffix(string(output), "\x00"), "\x00"); !slices.Equal(got, want) {
		t.Fatalf("argv = %q, want %q", got, want)
	}
}
