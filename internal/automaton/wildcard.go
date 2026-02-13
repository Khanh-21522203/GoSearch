package automaton

import "errors"

// Wildcard pattern limits.
const MaxWildcardPatternLength = 256

var (
	ErrWildcardPatternTooLong = errors.New("wildcard pattern exceeds maximum length")
	ErrDFAStateLimitExceeded  = errors.New("DFA state limit exceeded during construction")
)

// WildcardAutomaton accepts strings matching a wildcard pattern.
// Supports '*' (zero or more characters) and '?' (exactly one character).
//
// Construction converts the pattern to a DFA via NFA subset construction.
type WildcardAutomaton struct {
	// transitions[state][byte] = next state
	transitions [][]State
	accepting   []bool
}

// NewWildcardAutomaton compiles a wildcard pattern into a DFA.
func NewWildcardAutomaton(pattern []byte) (*WildcardAutomaton, error) {
	if len(pattern) > MaxWildcardPatternLength {
		return nil, ErrWildcardPatternTooLong
	}

	// Build NFA then convert to DFA via subset construction.
	nfa := buildWildcardNFA(pattern)
	return subsetConstruct(nfa)
}

func (a *WildcardAutomaton) Start() State {
	return 1 // State 1 is start; 0 is dead.
}

func (a *WildcardAutomaton) Step(state State, b byte) State {
	if state == DeadState || int(state) >= len(a.transitions) {
		return DeadState
	}
	return a.transitions[state][b]
}

func (a *WildcardAutomaton) IsAccept(state State) bool {
	if state == DeadState || int(state) >= len(a.accepting) {
		return false
	}
	return a.accepting[state]
}

func (a *WildcardAutomaton) CanMatch(state State) bool {
	return state != DeadState
}

// --- NFA representation for wildcard patterns ---

type nfaState struct {
	transitions [256][]int // byte → set of next states
	epsilon     []int      // ε-transitions
	accepting   bool
}

type nfa struct {
	states []*nfaState
}

func newNFAState() *nfaState {
	return &nfaState{}
}

func buildWildcardNFA(pattern []byte) *nfa {
	n := &nfa{}
	start := newNFAState()
	n.states = append(n.states, start)

	current := 0
	for _, ch := range pattern {
		next := len(n.states)
		nextState := newNFAState()
		n.states = append(n.states, nextState)

		switch ch {
		case '*':
			// ε-transition to next (skip) + self-loop on any byte
			n.states[current].epsilon = append(n.states[current].epsilon, next)
			for b := 0; b < 256; b++ {
				nextState.transitions[b] = append(nextState.transitions[b], next)
			}
			// Also allow ε from next to skip the star entirely
			// The star state loops on itself
			current = next
		case '?':
			// Any single byte transitions to next state
			for b := 0; b < 256; b++ {
				n.states[current].transitions[b] = append(n.states[current].transitions[b], next)
			}
			current = next
		default:
			// Exact byte match
			n.states[current].transitions[ch] = append(n.states[current].transitions[ch], next)
			current = next
		}
	}

	n.states[current].accepting = true
	return n
}

// subsetConstruct converts an NFA to a DFA using the subset construction algorithm.
// Returns an error if the DFA exceeds MaxDFAStates.
func subsetConstruct(n *nfa) (*WildcardAutomaton, error) {
	type stateSet map[int]bool

	epsilonClosure := func(states stateSet) stateSet {
		closure := make(stateSet)
		stack := make([]int, 0, len(states))
		for s := range states {
			closure[s] = true
			stack = append(stack, s)
		}
		for len(stack) > 0 {
			s := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			for _, eps := range n.states[s].epsilon {
				if !closure[eps] {
					closure[eps] = true
					stack = append(stack, eps)
				}
			}
		}
		return closure
	}

	setKey := func(s stateSet) uint64 {
		// Simple hash for state set identity.
		var h uint64
		for k := range s {
			h ^= uint64(k)*2654435761 + uint64(k)
		}
		return h
	}

	isAccepting := func(s stateSet) bool {
		for k := range s {
			if n.states[k].accepting {
				return true
			}
		}
		return false
	}

	// DFA state 0 = dead, state 1 = start
	dfa := &WildcardAutomaton{
		transitions: [][]State{make([]State, 256)}, // dead state
		accepting:   []bool{false},
	}

	startSet := epsilonClosure(stateSet{0: true})
	startTrans := make([]State, 256)
	dfa.transitions = append(dfa.transitions, startTrans)
	dfa.accepting = append(dfa.accepting, isAccepting(startSet))

	// Map from state set hash → DFA state ID
	setToID := map[uint64]State{setKey(startSet): 1}
	queue := []stateSet{startSet}
	queueIDs := []State{1}

	for len(queue) > 0 {
		currentSet := queue[0]
		currentID := queueIDs[0]
		queue = queue[1:]
		queueIDs = queueIDs[1:]

		for b := 0; b < 256; b++ {
			nextSet := make(stateSet)
			for s := range currentSet {
				for _, target := range n.states[s].transitions[b] {
					nextSet[target] = true
				}
			}

			if len(nextSet) == 0 {
				dfa.transitions[currentID][b] = DeadState
				continue
			}

			nextSet = epsilonClosure(nextSet)
			key := setKey(nextSet)

			if id, exists := setToID[key]; exists {
				dfa.transitions[currentID][b] = id
			} else {
				newID := State(len(dfa.transitions))
				if int(newID) >= MaxDFAStates {
					return nil, ErrDFAStateLimitExceeded
				}
				setToID[key] = newID
				dfa.transitions = append(dfa.transitions, make([]State, 256))
				dfa.accepting = append(dfa.accepting, isAccepting(nextSet))
				dfa.transitions[currentID][b] = newID
				queue = append(queue, nextSet)
				queueIDs = append(queueIDs, newID)
			}
		}
	}

	return dfa, nil
}
