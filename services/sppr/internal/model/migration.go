package model

import (
	"errors"
	"time"
)

var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrActionNotFound  = errors.New("migration action not found")
	// ErrActionConflict: an action_id is already used with other fields.
	ErrActionConflict = errors.New("migration action conflicts with an accepted one")
	// ErrActionStateChanged: the action left the state a transition expected.
	ErrActionStateChanged = errors.New("migration action state changed")
	ErrPinNotFound        = errors.New("pin not found")
	ErrPinExists          = errors.New("pin already exists")
)

// Errors of Metadata and Storage calls, mapped from gRPC codes by the clients.
var (
	ErrUpstreamUnavailable  = errors.New("upstream unavailable")
	ErrUpstreamNotFound     = errors.New("upstream object not found")
	ErrStale                = errors.New("version is not current or placement changed")
	ErrNoCapacity           = errors.New("no capacity in storage class")
	ErrUpstreamPrecondition = errors.New("upstream precondition failed")
)

type StorageClass string

const (
	StorageClassHot  StorageClass = "HOT"
	StorageClassCold StorageClass = "COLD"
)

func (c StorageClass) Valid() bool { return c == StorageClassHot || c == StorageClassCold }

type ActionStatus string

const (
	ActionPending   ActionStatus = "pending"
	ActionRunning   ActionStatus = "running"
	ActionSucceeded ActionStatus = "succeeded"
	ActionFailed    ActionStatus = "failed"
	ActionCancelled ActionStatus = "cancelled"
)

func (s ActionStatus) Valid() bool {
	switch s {
	case ActionPending, ActionRunning, ActionSucceeded, ActionFailed, ActionCancelled:
		return true
	}
	return false
}

// Unsuccessful reports a final status that cancels the dependent actions.
func (s ActionStatus) Unsuccessful() bool { return s == ActionFailed || s == ActionCancelled }

func (s ActionStatus) Terminal() bool { return s == ActionSucceeded || s.Unsuccessful() }

type ActionStep string

const (
	StepNone      ActionStep = ""
	StepReserving ActionStep = "reserving"
	StepCopying   ActionStep = "copying"
	StepSwitching ActionStep = "switching"
	// StepAborting keeps the failure reason while AbortCopy is pending, so a
	// restart finishes the abort instead of copying again.
	StepAborting ActionStep = "aborting"
)

type Reason string

const (
	ReasonNone Reason = ""

	ReasonStale               Reason = "stale"
	ReasonNoCapacity          Reason = "no_capacity"
	ReasonVerifyMismatch      Reason = "verify_mismatch"
	ReasonSourceMissing       Reason = "source_missing"
	ReasonUpstreamUnavailable Reason = "upstream_unavailable"
	ReasonInternal            Reason = "internal"

	ReasonUser             Reason = "user"
	ReasonPinned           Reason = "pinned"
	ReasonMinInterval      Reason = "min_interval"
	ReasonDependencyFailed Reason = "dependency_failed"
)

type BlockedBy string

const (
	BlockedByNone         BlockedBy = ""
	BlockedByDependencies BlockedBy = "dependencies"
	BlockedByBudget       BlockedBy = "budget"
	BlockedByPaused       BlockedBy = "paused"
)

// ActionSpec is what a caller submits; ActionID is its idempotency key and the
// migration_id in Metadata.
type ActionSpec struct {
	ActionID           string
	PlanID             string
	VersionID          string
	ExpectedGeneration int64
	SourceClass        StorageClass
	TargetClass        StorageClass
	SizeBytes          int64
	DependsOn          []string
}

type Action struct {
	ActionSpec
	Status       ActionStatus
	Step         ActionStep
	Reason       Reason
	Attempts     int32
	BytesCharged int64
	// OpenCharge: a budget charge is reserved for a copy attempt not yet settled.
	OpenCharge bool
	// DepsPending: some dependency has not succeeded yet.
	DepsPending bool
	CreatedAt   time.Time
	StartedAt   *time.Time
	FinishedAt  *time.Time
	UpdatedAt   time.Time
}

type ActionEvent struct {
	At         time.Time
	FromStatus ActionStatus
	ToStatus   ActionStatus
	FromStep   ActionStep
	ToStep     ActionStep
	Bytes      int64
	Reason     Reason
	Error      string
}

type ActionDetails struct {
	Action    *Action
	BlockedBy BlockedBy
	Events    []ActionEvent
}

type ActionView struct {
	Action    *Action
	BlockedBy BlockedBy
}

type ActionFilter struct {
	PlanID    string
	Status    ActionStatus
	PageSize  int
	PageToken string
}

type ActionPage struct {
	Actions       []ActionView
	NextPageToken string
}

type SubmitResult struct {
	Accepted []string
	Existing []string
}

// CancelRequest selects pending actions by plan or by IDs, not both.
type CancelRequest struct {
	PlanID    string
	ActionIDs []string
}

type CancelResult struct {
	Cancelled []string
	// Dependents were pending on a cancelled action and got dependency_failed.
	Dependents []string
}

// Transition moves a running action and settles its budget in one transaction.
type Transition struct {
	ActionID   string
	FromStatus ActionStatus
	FromStep   ActionStep
	Status     ActionStatus
	Step       ActionStep
	Reason     Reason
	// Settle closes the open charge with Bytes actually transferred (0 refunds it).
	Settle bool
	Bytes  int64
	// Reopen charges the planned size for the next copy attempt.
	Reopen bool
	Error  string
}

type MigrationSettings struct {
	BudgetBytes  int64
	BudgetWindow time.Duration
	MinInterval  time.Duration
	Paused       bool
}

type BudgetStatus struct {
	Bytes     int64
	Window    time.Duration
	Spent     int64
	Remaining int64
}

type Pin struct {
	Bucket    string
	Key       string
	Reason    string
	Author    string
	CreatedAt time.Time
}

// Placement and PlacementCopy mirror Metadata PlacementService messages.
type Placement struct {
	VersionID  string
	Bucket     string
	Key        string
	IsCurrent  bool
	Class      StorageClass
	BlobID     string
	Generation int64
	SizeBytes  int64
	UpdatedAt  time.Time
}

type CopyRole string

const (
	CopyRoleTarget CopyRole = "target"
	CopyRoleSource CopyRole = "source"
)

type CopyState string

const (
	CopyReserved       CopyState = "reserved"
	CopyPromoted       CopyState = "promoted"
	CopyCleanupPending CopyState = "cleanup_pending"
	CopyCleanupFailed  CopyState = "cleanup_failed"
	CopyDeleted        CopyState = "deleted"
)

type PlacementCopy struct {
	CopyID             string
	Role               CopyRole
	Class              StorageClass
	BlobID             string
	SizeBytes          int64
	ExpectedGeneration int64
	ExpectedClass      StorageClass
	State              CopyState
}

type SwitchRequest struct {
	MigrationID        string
	VerifiedSizeBytes  int64
	VerifiedMD5        string
	SourceCleanupDelay time.Duration
}
