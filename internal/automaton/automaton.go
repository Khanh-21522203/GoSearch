package automaton

// State represents a state in a deterministic finite automaton.
type State uint32

// DeadState is the sink state from which no accepting state is reachable.
const DeadState State = 0

// Automaton is the core interface for all DFA-based query expansion.
// All non-trivial term expansion (prefix, wildcard, regex, fuzzy) MUST be
// executed as Automaton ∩ FST intersection.
//
// Properties:
//   - Deterministic: single transition per (state, input)
//   - Finite: bounded state count
//   - No ε-transitions
type Automaton interface {
	// Start returns the initial state.
	Start() State

	// Step returns the next state for the given input byte.
	// Returns DeadState if no transition exists.
	Step(state State, b byte) State

	// IsAccept returns true if the state is an accepting state.
	IsAccept(state State) bool

	// CanMatch returns true if any accepting state is reachable from this state.
	// Used for pruning during FST intersection.
	CanMatch(state State) bool
}
