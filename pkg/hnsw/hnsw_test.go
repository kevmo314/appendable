package hnsw

import (
	"testing"
)

func numNodes(h *Hnsw) ([]NodeId, int) {
	var nodeIds []NodeId
	for k := range h.Nodes {
		nodeIds = append(nodeIds, k)
	}

	return nodeIds, len(nodeIds)
}

func TestHnsw(t *testing.T) {
	t.Run("builds graph", func(t *testing.T) {
		n := NewNode(0, []float64{0.1, 0.2}, 3)
		h := NewHNSW(32, 32, n)

		if h.MaxLevel != n.level {
			t.Fatalf("expected max level to default to %v, got %v", n.level, h.MaxLevel)
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

		h := NewHNSW(32, 1, NewNode(0, []float64{0, 0}, 3))

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

		h := NewHNSW(32, 1, NewNode(0, []float64{0, 0}, 3))

		res, err := h.selectNeighbors(candidates, 10)
		if err != nil || res.Len() != 3 {
			t.Fatal("if num neighbors to return is greater than candidates, we should just be returning the candidates")
		}
	})
}

func TestHnsw_Insert(t *testing.T) {

	t.Run("nodes[0] is root", func(t *testing.T) {
		n := NewNode(0, []float64{11, 11}, 3)
		h := NewHNSW(32, 32, n)

		if len(h.Nodes) != 1 {
			t.Fatalf("hnsw should be initialized with root node but got len: %v", len(h.Nodes))
		}

		if h.Nodes[0].id != 0 {
			t.Fatalf("expected node id at 0 to be initialized but got %v", h.Nodes[0].id)
		}
	})

	t.Run("hnsw with inserted element q", func(t *testing.T) {
		entryNode := NewNode(0, []float64{1, 1, 1}, 3)
		h := NewHNSW(32, 32, entryNode)

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

		if h.Nodes[1].id != 1 {
			t.Fatalf("expected node id at 1 to be initialized but got %v", h.Nodes[1].id)
		}

		if EuclidDist(h.Nodes[1].v, []float64{1.3, 2.5, 2.3}) != 0 {
			t.Fatalf("incorrect vector inserted at %v expected vector %v but got %v", 1, []float64{1.3, 2.5, 2.3}, h.Nodes[1].v)
		}
	})

	t.Run("multiple insert", func(t *testing.T) {
		h := NewHNSW(10, 10, NewNode(0, []float64{0, 0}, 40))

		for i := 0; i < 32; i++ {
			if len(h.Nodes) != i+1 {
				t.Fatalf("expected the number of nodes in graph to be %v, got %v", i+1, len(h.Nodes))
			}

			if err := h.Insert([]float64{float64(32 - i), float64(31 - i)}); err != nil {
				t.Fatal(err)
			}

			if len(h.Nodes) != i+2 {
				t.Fatalf("expected the number of nodes in graph to be %v, got %v", i+2, len(h.Nodes)+2)
			}
		}

		items, err := h.KnnSearch([]float64{32, 31}, 10, 32)
		if err != nil {
			return
		}

		if items.Len() != 10 {
			t.Fatalf("expected to return %v neighbors, got: %v", 10, items.Len())
		}

		expectedId := NodeId(1)

		for !items.IsEmpty() {
			peeled, err := items.Peel()

			if err != nil {
				t.Fatal(err)
			}

			if peeled.id != expectedId {
				t.Fatalf("expected %v, but got %v", expectedId, peeled.id)
			}
		}

	})

	t.Run("multiple insert 2", func(t *testing.T) {
		h := NewHNSW(10, 10, NewNode(0, []float64{0, 0, 0, 0}, 100))

		if h.MaxLevel != 100 {
			t.Fatalf("expected max level to be %v, got %v", 100, h.MaxLevel)
		}

		if _, numNodes := numNodes(h); numNodes != 1 {
			t.Fatalf("expected to return 1 node but got %v", numNodes)
		}

		for i := 0; i < 32; i++ {
			q := []float64{float64(32 - i), float64(31 - i), float64(32 - i), float64(31 - 1)}
			if err := h.Insert(q); err != nil {
				t.Fatal(err)
			}

			if _, numNodes := numNodes(h); numNodes != i+2 {
				t.Fatalf("expected to return %v node but got %v", i+2, numNodes)
			}

			currNodeId := NodeId(i + 1)
			if !NearlyEqual(h.Nodes[currNodeId].VecDistFromVec(q), 0) {
				t.Fatalf("expected at id %v, for vec to be q but got %v", currNodeId, h.Nodes[currNodeId].VecDistFromVec(q))
			}

		}
	})
}

func TestHnsw_Link(t *testing.T) {
	t.Run("links correctly", func(t *testing.T) {

		n1 := NewNode(1, make(Vector, 128), 3)
		n2 := NewNode(2, make(Vector, 128), 0)

		p := make(Vector, 128)
		h := NewHNSW(4, 200, NewNode(0, p, 3))

		h.Nodes[1] = n1
		h.Nodes[2] = n2

		i1 := Item{id: 1, dist: 3}

		// now h has enuogh context to test Linking

		if len(h.Nodes[1].friends) != 4 {
			t.Fatalf("node1 has max layer 3 so 4 layers total, got %v", len(h.Nodes[1].friends))
		}

		h.Link(&i1, n2, 0)

		if h.Nodes[1].friends[0].Len() != 1 {
			t.Fatalf("expected n1's num friends at level 1 to be 1, got %v", h.Nodes[1].friends[1].Len())
		}

		if h.Nodes[2].friends[0].Len() != 1 {
			t.Fatalf("expected n2's num friends at level 1 to be 1, got %v", h.Nodes[1].friends[1].Len())
		}

		peeled, err := h.Nodes[1].friends[0].Peel()
		if err != nil {
			t.Fatal(err)
		}

		if peeled.id != 2 {
			t.Fatalf("expected n1 to be friends with n2 at level 1")
		}

		peeled, err = h.Nodes[2].friends[0].Peel()

		if err != nil {
			t.Fatal(err)
		}

		if peeled.id != 1 {
			t.Fatalf("expected n1 to be friends with n1 at level 1")
		}

	})

	t.Run("links correctly 2", func(t *testing.T) {
		qNode := NewNode(1, []float64{4, 4}, 3)

		h := NewHNSW(1, 23, NewNode(0, []float64{0, 0}, 10))

		h.Nodes[qNode.id] = qNode

		friends := [][]float64{
			{2, 2}, {3, 3}, {3.5, 3.5},
		}

		for i, v := range friends {
			id := NodeId(i + 2)
			h.Nodes[id] = NewNode(id, v, 2)

			if len(h.Nodes[id].friends) != 3 {
				t.Fatalf("only initialized so expected qfriend to have size 0 friend map, got: %v", len(h.Nodes[id].friends))
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

func TestNextNodeId(t *testing.T) {
	t.Run("generate next node", func(t *testing.T) {
		h := NewHNSW(30, 30, NewNode(0, []float64{}, 1))
		for i := 0; i <= 100; i++ {
			nextNodeId := h.getNextNodeId()

			if nextNodeId != NodeId(i+1) {
				t.Fatalf("expected %v, got %v", i+1, nextNodeId)
			}
		}
	})
}

func TestFindCloserEntryPoint(t *testing.T) {
	t.Run("find nothing closer", func(t *testing.T) {
		epNode := NewNode(0, []float64{0, 0}, 10)
		h := NewHNSW(32, 32, epNode)

		qVector := []float64{6, 6}
		qLevel := h.spawnLevel()

		epItem := &Item{id: 0, dist: epNode.VecDistFromVec(qVector)}
		newEpItem := h.findCloserEntryPoint(epItem, qVector, qLevel)

		if epItem.id != newEpItem.id {
			t.Fatalf("expected id to be %v, got %v", newEpItem.id, epItem.id)
		}
	})

	t.Run("finds something closer traverse all layers", func(t *testing.T) {
		ep := NewNode(0, []float64{0, 0}, 10)
		h := NewHNSW(32, 32, ep)

		q := []float64{6, 6}

		// suppose we had m := []float{5, 5}. It is closer to q, so let's add m to the friends of ep

		m := NewNode(1, []float64{5, 5}, 9)
		h.Nodes[m.id] = m

		for level := 0; level <= 9; level++ {
			ep.InsertFriendsAtLevel(level, m.id, m.VecDistFromVec(q))
		}

		epItem := &Item{id: 0, dist: ep.VecDistFromVec(q)}
		newEpItem := h.findCloserEntryPoint(epItem, q, 0)

		if epItem.id == newEpItem.id {
			t.Fatalf("expected id to be %v, got %v", newEpItem.id, epItem.id)
		}

		if newEpItem.id != 1 {
			t.Fatalf("expected id to be 1, got %v", newEpItem.id)
		}
	})

	t.Run("finds something closer during the insertion context", func(t *testing.T) {
		ep := NewNode(0, []float64{0, 0}, 10)
		h := NewHNSW(32, 32, ep)

		q := []float64{6, 6}
		qLayer := 3

		// suppose we had m := []float{5, 5}. It is closer to q, so let's add m to the friends of ep
		m := NewNode(1, []float64{5, 5}, 9)
		h.Nodes[m.id] = m
		mDist := m.VecDistFromVec(q)

		h.Link(&Item{id: m.id, dist: mDist}, h.Nodes[h.EntryNodeId], m.level)

		n := NewNode(2, []float64{6.1, 6.1}, 4)
		h.Nodes[n.id] = n
		nDist := n.VecDistFromNode(m)
		h.Link(&Item{id: n.id, dist: nDist}, m, n.level)

		// verify for entry node's friends
		friends := h.Nodes[h.EntryNodeId].friends
		if friends[9].IsEmpty() {
			t.Fatalf("expected friends to not be empty at level 4, got %v", friends[4].Len())
		}
		if friends[9].Peek().id != 1 {
			t.Fatalf("expected friend id at level 9 to be %v, got %v", 1, friends[9].Peek().id)
		}

		nextFriends := h.Nodes[1].friends
		if nextFriends[4].IsEmpty() {
			t.Fatalf("expected friends to not be empty at level 4, got %v", friends[4].Len())
		}

		if nextFriends[4].Peek().id != 2 {
			t.Fatalf("expected friend id at level 4 to be %v, got %v", 2, friends[4].Peek().id)
		}

		epItem := &Item{id: 0, dist: ep.VecDistFromVec(q)}
		newEpItem := h.findCloserEntryPoint(epItem, q, qLayer)

		if epItem.id == newEpItem.id {
			t.Fatalf("expected id to be %v, got %v", newEpItem.id, epItem.id)
		}

		if newEpItem.id != n.id {
			t.Fatalf("expected id to be %v, got %v", n.id, newEpItem.id)
		}
	})
}
