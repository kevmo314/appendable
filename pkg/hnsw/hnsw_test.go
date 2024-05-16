package hnsw

import (
	"fmt"
	"testing"
)

func TestHnsw(t *testing.T) {
	t.Run("builds graph", func(t *testing.T) {
		n := NewNode(0, []float64{0.1, 0.2}, 3)
		h := NewHNSW(20, 32, 32, n)

		if h.MaxLevel != -1 {
			t.Fatalf("expected max level to default to -1, got %v", h.MaxLevel)
		}
	})
}

func TestHnswSelect(t *testing.T) {

	t.Run("selects m nearest elements to q", func(t *testing.T) {
		candidates := FromBaseQueue([]*Item{
			{id: 1, dist: 30},
			{id: 2, dist: 29},
			{id: 3, dist: 28},
			{id: 4, dist: 27},
			{id: 5, dist: 26},
			{id: 6, dist: 25},
			{id: 7, dist: 24},
			{id: 8, dist: 23},
			{id: 9, dist: 22},
			{id: 10, dist: 21},
			{id: 11, dist: 20},
		}, MinComparator{})

		h := NewHNSW(2, 32, 1, NewNode(0, []float64{0, 0}, 3))

		cn, err := h.selectNeighbors(candidates, 10)

		if err != nil {
			t.Fatal(err)
		}

		if cn.Len() != 10 {
			t.Fatalf("did not take 10 items")
		}

		expected := 11
		i := 0
		for !cn.IsEmpty() {
			peeled, err := cn.Peel()
			if err != nil {
				t.Fatal(err)
			}
			if peeled.id != NodeId(expected) {
				t.Fatalf("expected %v, but got %v at %v", expected, peeled.id, i)
			}

			expected--
			i++
		}
	})

	t.Run("over selects! greedy", func(t *testing.T) {
		candidates := FromBaseQueue([]*Item{
			{id: 1, dist: 30},
			{id: 2, dist: 0.6},
			{id: 3, dist: 8},
		}, MinComparator{})

		h := NewHNSW(2, 32, 1, NewNode(0, []float64{0, 0}, 3))

		_, err := h.selectNeighbors(candidates, 10)
		if err == nil {
			t.Fatalf("expected to fail!")
		}
	})
}

func TestHnsw_Insert(t *testing.T) {

	t.Run("nodes[0] is root", func(t *testing.T) {
		n := NewNode(0, []float64{11, 11}, 3)
		h := NewHNSW(2000, 32, 32, n)

		if len(h.Nodes) != 1 {
			t.Fatalf("hnsw should be initialized with root node but got len: %v", len(h.Nodes))
		}

		if h.Nodes[0].id != 0 {
			t.Fatalf("expected node id at 0 to be initialized but got %v", h.Nodes[0].id)
		}
	})

	t.Run("hnsw with inserted element q", func(t *testing.T) {
		entryNode := NewNode(0, []float64{1, 1, 1}, 3)
		h := NewHNSW(3, 32, 32, entryNode)

		if len(h.Nodes) != 1 {
			t.Fatalf("hnsw should be initialized with root node but got len: %v", len(h.Nodes))
		}

		err := h.Insert([]float64{1.3, 2.5, 2.3})
		if err != nil {
			return
		}

		if len(h.Nodes) != 2 {
			t.Fatalf("expected 2 nodes after insertion but got %v", len(h.Nodes))
		}

		fmt.Printf("new node %v", h.Nodes[1])

		if h.Nodes[1].id != 1 {
			t.Fatalf("expected node id at 1 to be initialized but got %v", h.Nodes[1].id)
		}

		if EuclidDist(h.Nodes[1].v, []float64{1.3, 2.5, 2.3}) != 0 {
			t.Fatalf("incorrect vector inserted at %v expected vector %v but got %v", 1, []float64{1.3, 2.5, 2.3}, h.Nodes[1].v)
		}
	})

	t.Run("verify insert", func(t *testing.T) {
		h := NewHNSW(2000, 32, 32, NewNode(0, []float64{1, 1, 1}, 10))

		if h.MaxLayer != 10 {
			t.Fatalf("expected max layer to be set greater than 0, but got %v", h.MaxLayer)
		}

		if err := h.Insert([]float64{3, 3, 3}); err != nil {
			t.Fatal(err)
		}
	})

}

func TestHnsw_Link(t *testing.T) {
	t.Run("links correctly", func(t *testing.T) {

		mq1 := make(map[int]*BaseQueue)
		mq1[0] = NewBaseQueue(MinComparator{})
		mq1[1] = NewBaseQueue(MinComparator{})
		mq1[2] = NewBaseQueue(MinComparator{})

		mq2 := make(map[int]*BaseQueue)
		mq2[0] = NewBaseQueue(MinComparator{})
		mq2[1] = NewBaseQueue(MinComparator{})

		n1 := Node{
			id:      1,
			v:       make(Vector, 128),
			level:   3,
			friends: mq1,
		}

		n2 := Node{
			id:      2,
			v:       make(Vector, 128),
			level:   0,
			friends: mq2,
		}

		p := make(Vector, 128)
		h := NewHNSW(128, 4, 200, NewNode(0, p, 3))

		h.Nodes[1] = &n1
		h.Nodes[2] = &n2

		i1 := Item{id: 1, dist: 3}

		// now h has enuogh context to test Linking

		if h.Nodes[1].friends[1].Len() != 0 {
			t.Fatalf("expected n1's num friends at level 1 to be 0, got %v", h.Nodes[1].friends[1].Len())
		}

		if h.Nodes[2].friends[1].Len() != 0 {
			t.Fatalf("expected n2's num friends at level 1 to be 0, got %v", h.Nodes[1].friends[1].Len())
		}

		h.Link(&i1, &n2, 1)

		// i1 should be friends with i2
		// i2 should be friends with i1

		if h.Nodes[1].friends[1].Len() != 1 {
			t.Fatalf("expected n1's num friends at level 1 to be 1, got %v", h.Nodes[1].friends[1].Len())
		}

		if h.Nodes[2].friends[1].Len() != 1 {
			t.Fatalf("expected n2's num friends at level 1 to be 1, got %v", h.Nodes[1].friends[1].Len())
		}

		peeled, err := h.Nodes[1].friends[1].Peel()
		if err != nil {
			t.Fatal(err)
		}

		if peeled.id != 2 {
			t.Fatalf("expected n1 to be friends with n2 at level 1")
		}

		peeled, err = h.Nodes[2].friends[1].Peel()

		if err != nil {
			t.Fatal(err)
		}

		if peeled.id != 1 {
			t.Fatalf("expected n1 to be friends with n1 at level 1")
		}

	})

	t.Run("links correctly 2", func(t *testing.T) {
		qNode := NewNode(1, []float64{4, 4}, 3)

		h := NewHNSW(2, 1, 23, NewNode(0, []float64{0, 0}, 10))

		h.Nodes[qNode.id] = qNode

		friends := [][]float64{
			{2, 2}, {3, 3}, {3.5, 3.5},
		}

		for i, v := range friends {
			id := NodeId(i + 2)
			h.Nodes[id] = NewNode(id, v, 2)

			if len(h.Nodes[id].friends) != 0 {
				t.Fatalf("only initialized so expected qfriend to have size 0 friend map")
			}
		}

		// add some friends for qnode at level 2
		qNode.InsertFriendsAtLevel(2, 2, qNode.VecDistFromNode(h.Nodes[2]))
		qNode.InsertFriendsAtLevel(2, 3, qNode.VecDistFromNode(h.Nodes[3]))
		qNode.InsertFriendsAtLevel(2, 4, qNode.VecDistFromNode(h.Nodes[4]))

		qFriendsAtLevel2 := qNode.GetFriendsAtLevel(2)
		if qFriendsAtLevel2.Len() != 3 {
			t.Fatalf("expected qFriendsAtLevel2 to be 3, got %v", qFriendsAtLevel2.Len())
		}

		// we pop since link adds bidirectional
		for !qFriendsAtLevel2.IsEmpty() {
			peeled, err := qFriendsAtLevel2.Peel()
			if err != nil {
				t.Fatal(err)
			}

			if peeled.id != NodeId(qFriendsAtLevel2.Len()+2) {
				t.Fatalf("expected peeled id to be %v got %v", qFriendsAtLevel2.Len()+2, peeled.id)
			}
		}

		for i, v := range friends {
			id := NodeId(i + 2)
			dist := qNode.VecDistFromVec(v)

			h.Link(&Item{id: id, dist: dist}, qNode, 2)

			qFriendNode := h.Nodes[id]
			friendsAtLevel2 := qFriendNode.GetFriendsAtLevel(2)

			if friendsAtLevel2.Len() != 1 {
				t.Fatalf("expected friends at level 2 to be 1, got %v", friendsAtLevel2.Len())
			}

			qFriendFriend, err := friendsAtLevel2.Peel()
			if err != nil {
				t.Fatal(err)
			}

			if qFriendFriend.id != qNode.id {
				t.Fatalf("expected friend id at level 2 to be q node 1, got %v", qFriendFriend.id)
			}
		}
	})
}
