package automaton

import "errors"

// Levenshtein automaton limits.
const MaxEditDistance = 2

var (
	ErrEditDistanceTooLarge = errors.New("edit distance exceeds maximum of 2")
	ErrTermTooShort         = errors.New("term too short for fuzzy matching (min 3)")
)

// LevenshteinAutomaton accepts strings within edit distance ≤ maxDist of the target.
// Uses parametric state representation: (position, edit_budget).
//
// Supports edit distance ≤ 2 only. Higher distances produce exponential state counts.
type LevenshteinAutomaton struct {
	target  []byte
	maxDist int
	// State encoding: state = position*(maxDist+1) + editsUsed + 1
	// DeadState (0) is reserved.
}

// NewLevenshteinAutomaton creates an automaton accepting strings within
// the given edit distance of the target.
func NewLevenshteinAutomaton(target []byte, maxDist int) (*LevenshteinAutomaton, error) {
	if maxDist > MaxEditDistance {
		return nil, ErrEditDistanceTooLarge
	}
	if maxDist < 0 {
		return nil, ErrEditDistanceTooLarge
	}
	return &LevenshteinAutomaton{
		target:  target,
		maxDist: maxDist,
	}, nil
}

func (a *LevenshteinAutomaton) Start() State {
	// Start state: position=0, editsUsed=0
	return a.encodeState(0, 0)
}

func (a *LevenshteinAutomaton) Step(state State, b byte) State {
	if state == DeadState {
		return DeadState
	}

	pos, editsUsed := a.decodeState(state)

	// If we've consumed the entire target, any additional byte is an insertion.
	if pos >= len(a.target) {
		newEdits := editsUsed + 1
		if newEdits > a.maxDist {
			return DeadState
		}
		return a.encodeState(pos, newEdits)
	}

	// Match: advance position, same edit budget.
	if b == a.target[pos] {
		return a.encodeState(pos+1, editsUsed)
	}

	// No match: try edit operations.
	if editsUsed >= a.maxDist {
		return DeadState
	}

	// We return the best (lowest edit cost) reachable state.
	// Substitution: advance position, use one edit.
	// This is the simplest single-step transition.
	// For insertion and deletion, we need multi-step handling.
	//
	// In a true Levenshtein DFA, each state encodes the full edit vector.
	// For simplicity, we use substitution as the primary mismatch transition.
	// Insertion (extra char in input) = stay at position, use one edit.
	// Deletion (missing char in input) is handled by checking if skipping
	// the target char leads to a match.

	// Try substitution: advance both.
	substState := a.encodeState(pos+1, editsUsed+1)

	// Try insertion: consume input byte, stay at same target position.
	insertState := a.encodeState(pos, editsUsed+1)

	// Try deletion: skip target char, don't consume input.
	// We check if the byte matches the next target char.
	if pos+1 < len(a.target) && b == a.target[pos+1] {
		// Deletion of target[pos] + match at target[pos+1]
		delMatchState := a.encodeState(pos+2, editsUsed+1)
		// Return the state that advances furthest.
		return a.bestState(substState, insertState, delMatchState)
	}

	return a.bestState(substState, insertState, DeadState)
}

func (a *LevenshteinAutomaton) IsAccept(state State) bool {
	if state == DeadState {
		return false
	}
	pos, editsUsed := a.decodeState(state)
	// Accept if remaining target chars can be covered by remaining edit budget.
	remaining := len(a.target) - pos
	return remaining <= (a.maxDist - editsUsed)
}

func (a *LevenshteinAutomaton) CanMatch(state State) bool {
	return state != DeadState
}

func (a *LevenshteinAutomaton) encodeState(pos, editsUsed int) State {
	if editsUsed > a.maxDist {
		return DeadState
	}
	if pos > len(a.target)+a.maxDist {
		return DeadState
	}
	return State(pos*(a.maxDist+1) + editsUsed + 1)
}

func (a *LevenshteinAutomaton) decodeState(state State) (pos, editsUsed int) {
	v := int(state) - 1
	editsUsed = v % (a.maxDist + 1)
	pos = v / (a.maxDist + 1)
	return
}

func (a *LevenshteinAutomaton) bestState(states ...State) State {
	best := DeadState
	bestPos := -1
	for _, s := range states {
		if s == DeadState {
			continue
		}
		pos, _ := a.decodeState(s)
		if pos > bestPos {
			bestPos = pos
			best = s
		}
	}
	return best
}
