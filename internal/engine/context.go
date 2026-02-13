package engine

import (
	"errors"
	"time"
)

var (
	ErrQueryTimeout       = errors.New("query execution timeout")
	ErrStateLimitExceeded = errors.New("automaton state limit exceeded")
	ErrMatchLimitExceeded = errors.New("term match limit exceeded")
)

// ExecutionContext tracks execution limits and timeout for a query.
type ExecutionContext struct {
	Deadline time.Time

	MaxStatesVisited int
	MaxTermsMatched  int

	StatesVisited int
	TermsMatched  int

	// checkCounter amortizes time checks.
	checkCounter  int
	checkInterval int

	TimedOut     bool
	LimitExceeded bool
}

// NewExecutionContext creates a context with the given timeout and limits.
func NewExecutionContext(timeout time.Duration, maxStates, maxTerms int) *ExecutionContext {
	if maxStates <= 0 {
		maxStates = 10000
	}
	if maxTerms <= 0 {
		maxTerms = 1000
	}
	interval := 128
	return &ExecutionContext{
		Deadline:         time.Now().Add(timeout),
		MaxStatesVisited: maxStates,
		MaxTermsMatched:  maxTerms,
		checkInterval:    interval,
	}
}

// CheckLimits checks whether any execution limit has been exceeded.
// Time checks are amortized to avoid calling time.Now() on every iteration.
func (ctx *ExecutionContext) CheckLimits() error {
	if ctx.StatesVisited >= ctx.MaxStatesVisited {
		ctx.LimitExceeded = true
		return ErrStateLimitExceeded
	}
	if ctx.TermsMatched >= ctx.MaxTermsMatched {
		ctx.LimitExceeded = true
		return ErrMatchLimitExceeded
	}

	ctx.checkCounter++
	if ctx.checkCounter%ctx.checkInterval == 0 {
		if time.Now().After(ctx.Deadline) {
			ctx.TimedOut = true
			return ErrQueryTimeout
		}
	}
	return nil
}
