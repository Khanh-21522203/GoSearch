package automaton

// PrefixAutomaton accepts all strings starting with a given prefix.
//
// States: 0..len(prefix), where state len(prefix) is the accepting state
// that loops on any byte.
type PrefixAutomaton struct {
	prefix []byte
}

// NewPrefixAutomaton creates an automaton that accepts strings with the given prefix.
func NewPrefixAutomaton(prefix []byte) *PrefixAutomaton {
	return &PrefixAutomaton{prefix: prefix}
}

func (a *PrefixAutomaton) Start() State {
	if len(a.prefix) == 0 {
		return 1 // Empty prefix: immediately accepting.
	}
	return 1 // State 1 is the start; DeadState (0) is dead.
}

func (a *PrefixAutomaton) Step(state State, b byte) State {
	if state == DeadState {
		return DeadState
	}
	pos := int(state) - 1 // state 1 = position 0
	if pos < len(a.prefix) {
		if b == a.prefix[pos] {
			return State(pos + 2) // advance to next state
		}
		return DeadState
	}
	// Past prefix: accept any byte, stay in accepting state.
	return state
}

func (a *PrefixAutomaton) IsAccept(state State) bool {
	if state == DeadState {
		return false
	}
	return int(state)-1 >= len(a.prefix)
}

func (a *PrefixAutomaton) CanMatch(state State) bool {
	return state != DeadState
}
