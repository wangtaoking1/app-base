package set

var (
	empty = struct{}{}
)

// Set is interface for set container.
type Set[E comparable] interface {
	// Add adds an element.
	Add(elem E)
	// AddAll adds all elememts.
	AddAll(elems ...E)
	// Remove removes the specific element.
	Remove(elem E)
	// RemoveAll removes all the specific elements.
	RemoveAll(elems ...E)
	// Size returns the size of the set.
	Size() int
	// Contains checks whether the element is in the set.
	Contains(elem E) bool
	// Empty checks whether the set is empty.
	Empty() bool
	// Values returns all elements list of the set.
	Values() []E
	// Clear clears the set.
	Clear()
}

type set[E comparable] struct {
	d map[E]struct{}
}

func (s *set[E]) Add(elem E) {
	s.d[elem] = empty
}

func (s *set[E]) AddAll(elems ...E) {
	for _, elem := range elems {
		s.d[elem] = empty
	}
}

func (s *set[E]) Remove(elem E) {
	delete(s.d, elem)
}

func (s *set[E]) RemoveAll(elems ...E) {
	for _, elem := range elems {
		delete(s.d, elem)
	}
}

func (s *set[E]) Size() int {
	return len(s.d)
}

func (s *set[E]) Empty() bool {
	return len(s.d) == 0
}

func (s *set[E]) Contains(elem E) bool {
	_, ok := s.d[elem]
	return ok
}

func (s *set[E]) Values() []E {
	values := make([]E, 0, len(s.d))
	for elem := range s.d {
		values = append(values, elem)
	}
	return values
}

func (s *set[E]) Clear() {
	s.d = make(map[E]struct{})
}

// New returns a new set container.
func New[E comparable](elems ...E) Set[E] {
	s := &set[E]{
		d: make(map[E]struct{}, len(elems)),
	}
	for _, elem := range elems {
		s.d[elem] = empty
	}
	return s
}
