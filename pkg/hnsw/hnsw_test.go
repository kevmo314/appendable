package hnsw

import "testing"

func TestHnsw(t *testing.T) {
	t.Run("builds graph", func(t *testing.T) {
		n := NewNode(0, Vector([]float64{0.1, 0.2}))
		h := NewHNSW(20, 32, 32, n)

		if h.MaxLayer != -1 {
			t.Fatalf("expected max layer to default to -1, got %v", h.MaxLayer)
		}
	})
}

func TestHnsw_Link(t *testing.T) {
	t.Run("links correctly", func(t *testing.T) {

		mq1 := make(map[int]*MinQueue)
		mq1[0] = NewMinQueue()
		mq1[1] = NewMinQueue()
		mq1[2] = NewMinQueue()

		mq2 := make(map[int]*MinQueue)
		mq2[0] = NewMinQueue()
		mq2[1] = NewMinQueue()

		n1 := Node{
			id:      1,
			v:       make(Vector, 128),
			layer:   3,
			friends: mq1,
		}

		n2 := Node{
			id:      2,
			v:       make(Vector, 128),
			layer:   0,
			friends: mq2,
		}

		p := make(Vector, 128)
		h := NewHNSW(128, 4, 200, NewNode(0, p))

		h.Nodes[1] = &n1
		h.Nodes[2] = &n2

		i1 := Item{id: 1, dist: 3}
		i2 := Item{id: 2, dist: 49}

		// now h has enuogh context to test Linking

		if h.Nodes[1].friends[1].Len() != 0 {
			t.Fatalf("expected n1's num friends at level 1 to be 0, got %v", h.Nodes[1].friends[1].Len())
		}

		if h.Nodes[2].friends[1].Len() != 0 {
			t.Fatalf("expected n2's num friends at level 1 to be 0, got %v", h.Nodes[1].friends[1].Len())
		}

		h.Link(&i1, &i2, 1)

		// i1 should be friends with i2
		// i2 should be friends with i1

		if h.Nodes[1].friends[1].Len() != 1 {
			t.Fatalf("expected n1's num friends at level 1 to be 1, got %v", h.Nodes[1].friends[1].Len())
		}

		if h.Nodes[2].friends[1].Len() != 1 {
			t.Fatalf("expected n2's num friends at level 1 to be 1, got %v", h.Nodes[1].friends[1].Len())
		}

		if h.Nodes[1].friends[1].Peel().id != 2 {
			t.Fatalf("expected n1 to be friends with n2 at level 1")
		}

		if h.Nodes[2].friends[1].Peel().id != 1 {
			t.Fatalf("expected n1 to be friends with n1 at level 1")
		}

	})
}
