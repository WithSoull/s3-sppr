//go:build integration

package action_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestClaimSkipsActionLockedByAnotherWorker(t *testing.T) {
	s := database(t)
	plan := uuid.NewString()
	first, second := spec(plan, 1), spec(plan, 1)
	s.insert(t, first)
	s.insert(t, second)

	held := make(chan *model.Action)
	release := make(chan struct{})
	done := make(chan error)
	go func() {
		done <- s.tx.ReadCommitted(context.Background(), func(ctx context.Context) error {
			a, err := s.actions.Claim(ctx, 100)
			if err != nil {
				return err
			}
			held <- a
			<-release
			return nil
		})
	}()
	a := <-held
	require.Equal(t, first.ActionID, a.ActionID)

	// Without SKIP LOCKED this claim would block on the uncommitted first one.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var other *model.Action
	require.NoError(t, s.tx.ReadCommitted(ctx, func(ctx context.Context) error {
		var err error
		other, err = s.actions.Claim(ctx, 100)
		return err
	}))
	require.NotNil(t, other)
	require.Equal(t, second.ActionID, other.ActionID)
	close(release)
	require.NoError(t, <-done)
	require.Nil(t, s.claim(t))
}

func TestConcurrentClaimsTakeEachActionOnce(t *testing.T) {
	s := database(t)
	plan := uuid.NewString()
	const total = 30
	for i := 0; i < total; i++ {
		s.insert(t, spec(plan, 1))
	}
	var mu sync.Mutex
	claimed := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				a := s.claim(t)
				if a == nil {
					return
				}
				mu.Lock()
				claimed[a.ActionID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	require.Len(t, claimed, total)
	for id, n := range claimed {
		require.Equal(t, 1, n, id)
	}
}

func TestConcurrentClaimsStayWithinBudget(t *testing.T) {
	s := database(t)
	require.NoError(t, s.settings.Update(context.Background(), model.MigrationSettings{
		BudgetBytes: 10, BudgetWindow: time.Hour, MinInterval: time.Hour,
	}))
	plan := uuid.NewString()
	for i := 0; i < 10; i++ {
		s.insert(t, spec(plan, 3))
	}
	var mu sync.Mutex
	var bytes int64
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for a := s.claim(t); a != nil; a = s.claim(t) {
				mu.Lock()
				bytes += a.SizeBytes
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(9), bytes)
	spent, err := s.settings.Spent(context.Background(), time.Hour)
	require.NoError(t, err)
	require.Equal(t, int64(9), spent)

	// A refund frees the budget for the next action.
	page, err := s.actions.List(context.Background(), model.ActionFilter{Status: model.ActionRunning, PageSize: 10})
	require.NoError(t, err)
	s.transition(t, model.Transition{ActionID: page[0].ActionID, FromStatus: model.ActionRunning, FromStep: model.StepReserving,
		Status: model.ActionCancelled, Reason: model.ReasonPinned, Settle: true, Bytes: 0})
	require.NotNil(t, s.claim(t))
	require.Nil(t, s.claim(t))
}

func TestDependentWaitsForPredecessor(t *testing.T) {
	s := database(t)
	plan := uuid.NewString()
	first := spec(plan, 1)
	second := spec(plan, 1, first.ActionID)
	s.insert(t, first, second)

	require.True(t, s.get(t, second.ActionID).DepsPending)
	a := s.claim(t)
	require.Equal(t, first.ActionID, a.ActionID)
	require.Nil(t, s.claim(t), "the dependent must wait while its predecessor runs")

	s.transition(t, model.Transition{ActionID: first.ActionID, FromStatus: model.ActionRunning, FromStep: model.StepReserving,
		Status: model.ActionSucceeded, Settle: true, Bytes: 1})
	require.False(t, s.get(t, second.ActionID).DepsPending)
	next := s.claim(t)
	require.NotNil(t, next)
	require.Equal(t, second.ActionID, next.ActionID)
}

func TestFailureCancelsDependentsTransitively(t *testing.T) {
	s := database(t)
	plan := uuid.NewString()
	first := spec(plan, 1)
	second := spec(plan, 1, first.ActionID)
	third := spec(plan, 1, second.ActionID)
	s.insert(t, first, second, third)
	unrelated := spec(plan, 1)
	s.insert(t, unrelated)
	require.Equal(t, first.ActionID, s.claim(t).ActionID)

	dependents := s.transition(t, model.Transition{ActionID: first.ActionID, FromStatus: model.ActionRunning, FromStep: model.StepReserving,
		Status: model.ActionFailed, Reason: model.ReasonNoCapacity, Settle: true})
	require.ElementsMatch(t, []string{second.ActionID, third.ActionID}, dependents)
	for _, id := range dependents {
		a := s.get(t, id)
		require.Equal(t, model.ActionCancelled, a.Status)
		require.Equal(t, model.ReasonDependencyFailed, a.Reason)
	}
	require.Equal(t, model.ActionPending, s.get(t, unrelated.ActionID).Status)
}

func TestTransitionChargesEveryCopyAttempt(t *testing.T) {
	s := database(t)
	a := spec(uuid.NewString(), 100)
	s.insert(t, a)
	require.NotNil(t, s.claim(t))

	steps := []model.Transition{
		{FromStatus: model.ActionRunning, FromStep: model.StepReserving, Status: model.ActionRunning, Step: model.StepCopying},
		// The first attempt broke after 40 bytes; the second is charged anew.
		{FromStatus: model.ActionRunning, FromStep: model.StepCopying, Status: model.ActionRunning, Step: model.StepCopying,
			Settle: true, Bytes: 40, Reopen: true, Error: "unavailable"},
		{FromStatus: model.ActionRunning, FromStep: model.StepCopying, Status: model.ActionRunning, Step: model.StepSwitching,
			Settle: true, Bytes: 100},
		{FromStatus: model.ActionRunning, FromStep: model.StepSwitching, Status: model.ActionSucceeded},
	}
	for _, tr := range steps {
		tr.ActionID = a.ActionID
		s.transition(t, tr)
	}
	got := s.get(t, a.ActionID)
	require.Equal(t, model.ActionSucceeded, got.Status)
	require.Equal(t, int32(2), got.Attempts)
	require.Equal(t, int64(140), got.BytesCharged)
	require.False(t, got.OpenCharge)
	require.NotNil(t, got.FinishedAt)
	spent, err := s.settings.Spent(context.Background(), time.Hour)
	require.NoError(t, err)
	require.Equal(t, int64(140), spent)

	events, err := s.actions.ListEvents(context.Background(), a.ActionID)
	require.NoError(t, err)
	require.Len(t, events, 6) // created, claimed and four transitions
	require.Equal(t, "unavailable", events[3].Error)
	require.Equal(t, int64(40), events[3].Bytes)

	err = s.tx.ReadCommitted(context.Background(), func(ctx context.Context) error {
		_, err := s.actions.Transition(ctx, model.Transition{ActionID: a.ActionID, FromStatus: model.ActionRunning,
			FromStep: model.StepSwitching, Status: model.ActionFailed, Reason: model.ReasonInternal})
		return err
	})
	require.ErrorIs(t, err, model.ErrActionStateChanged)
}

func TestCancelPendingByPlanAndIDs(t *testing.T) {
	s := database(t)
	plan, otherPlan := uuid.NewString(), uuid.NewString()
	running := spec(plan, 1)
	s.insert(t, running)
	require.NotNil(t, s.claim(t))
	pending := spec(plan, 1)
	other := spec(otherPlan, 1)
	s.insert(t, pending, other)

	var cancelled []string
	require.NoError(t, s.tx.ReadCommitted(context.Background(), func(ctx context.Context) error {
		var err error
		cancelled, err = s.actions.CancelPending(ctx, model.CancelRequest{PlanID: plan})
		return err
	}))
	require.Equal(t, []string{pending.ActionID}, cancelled, "running actions are finished, not cancelled")
	require.Equal(t, model.ReasonUser, s.get(t, pending.ActionID).Reason)

	require.NoError(t, s.tx.ReadCommitted(context.Background(), func(ctx context.Context) error {
		var err error
		cancelled, err = s.actions.CancelPending(ctx, model.CancelRequest{ActionIDs: []string{other.ActionID, running.ActionID}})
		return err
	}))
	require.Equal(t, []string{other.ActionID}, cancelled)
}
