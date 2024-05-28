package hnsw

import (
	"fmt"
	"reflect"
	"testing"
)

func TestHnsw(t *testing.T) {
	t.Run("builds graph", func(t *testing.T) {
		n := NewNode(0, []float32{0.1, 0.2}, 0)
		h := NewHNSW(2, 32, 32, []float32{0.1, 0.2})
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

		h := NewHNSW(2, 32, 1, []float32{0, 0})

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

		h := NewHNSW(2, 32, 1, []float32{0, 0})

		res, err := h.selectNeighbors(candidates, 10)
		if err != nil || res.Len() != 3 {
			t.Fatal("if num neighbors to return is greater than candidates, we should just be returning the candidates")
		}
	})
}

func TestHnsw_Insert(t *testing.T) {

	t.Run("nodes[0] is root", func(t *testing.T) {
		h := NewHNSW(2, 32, 32, []float32{11, 11})

		if len(h.Nodes) != 1 {
			t.Fatalf("hnsw should be initialized with root node but got len: %v", len(h.Nodes))
		}

		if h.Nodes[0].id != 0 {
			t.Fatalf("expected node id at 0 to be initialized but got %v", h.Nodes[0].id)
		}
	})

	t.Run("hnsw with inserted element q", func(t *testing.T) {
		h := NewHNSW(3, 32, 32, []float32{1, 1, 1})

		if len(h.Nodes) != 1 {
			t.Fatalf("hnsw should be initialized with root node but got len: %v", len(h.Nodes))
		}

		err := h.Insert([]float32{1.3, 2.5, 2.3})
		if err != nil {
			return
		}

		if len(h.Nodes) != 2 {
			t.Fatalf("expected 2 nodes after insertion but got %v", len(h.Nodes))
		}

		if h.Nodes[1].id != 1 {
			t.Fatalf("expected node id at 1 to be initialized but got %v", h.Nodes[1].id)
		}

		if EuclidDist(h.Nodes[1].v, []float32{1.3, 2.5, 2.3}) != 0 {
			t.Fatalf("incorrect vector inserted at %v expected vector %v but got %v", 1, []float32{1.3, 2.5, 2.3}, h.Nodes[1].v)
		}
	})

	t.Run("multiple insert", func(t *testing.T) {
		h := NewHNSW(2, 10, 10, []float32{0, 0})

		for i := 0; i < 32; i++ {
			if len(h.Nodes) != i+1 {
				t.Fatalf("expected the number of nodes in graph to be %v, got %v", i+1, len(h.Nodes))
			}

			if err := h.Insert([]float32{float32(32 - i), float32(31 - i)}); err != nil {
				t.Fatal(err)
			}

			if len(h.Nodes) != i+2 {
				t.Fatalf("expected the number of nodes in graph to be %v, got %v", i+2, len(h.Nodes)+2)
			}
		}

		items, err := h.KnnSearch([]float32{32, 31}, 10, 32)
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
}

func TestHnswVectorDimension(t *testing.T) {

	t.Run("create new hnsw", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Expected NewHNSW to panic due to mismatched dim, but it did not")
			}
		}()
		NewHNSW(3, 10, 8, []float32{1})
	})

	t.Run("insert mismatch vec", func(t *testing.T) {
		h := NewHNSW(3, 10, 8, []float32{1, 1, 1})

		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Expected NewHNSW to panic due to mismatched dim, but it did not")
			}
		}()
		err := h.Insert([]float32{})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestHnsw_Link(t *testing.T) {
	t.Run("links correctly", func(t *testing.T) {

		n1 := NewNode(1, make(Vector, 128), 3)
		n2 := NewNode(2, make(Vector, 128), 0)

		p := make(Vector, 128)
		h := NewHNSW(128, 4, 200, p)

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
		qNode := NewNode(1, []float32{4, 4}, 3)

		h := NewHNSW(2, 1, 23, []float32{0, 0})

		h.Nodes[qNode.id] = qNode

		friends := [][]float32{
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
		h := NewHNSW(0, 30, 30, []float32{})
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
		epNode := NewNode(0, []float32{0, 0}, 10)
		h := NewHNSW(2, 32, 32, []float32{0, 0})

		qVector := []float32{6, 6}
		qLevel := h.spawnLevel()

		epItem := &Item{id: 0, dist: epNode.VecDistFromVec(qVector)}
		newEpItem := h.findCloserEntryPoint(epItem, qVector, qLevel)

		if epItem.id != newEpItem.id {
			t.Fatalf("expected id to be %v, got %v", newEpItem.id, epItem.id)
		}
	})

	t.Run("finds something closer traverse all layers", func(t *testing.T) {
		ep := NewNode(0, []float32{0, 0}, 10)
		h := NewHNSW(2, 32, 32, []float32{0, 0})
		h.Nodes[0] = ep
		h.MaxLevel = 10

		q := []float32{6, 6}

		// suppose we had m := []float{5, 5}. It is closer to q, so let's add m to the friends of ep

		m := NewNode(1, []float32{5, 5}, 9)
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
		ep := NewNode(0, []float32{0, 0}, 10)

		h := NewHNSW(2, 32, 32, []float32{0, 0})
		h.MaxLevel = 10
		h.Nodes[0] = ep

		q := []float32{6, 6}
		qLayer := 3

		// suppose we had m := []float{5, 5}. It is closer to q, so let's add m to the friends of ep
		m := NewNode(1, []float32{5, 5}, 9)
		h.Nodes[m.id] = m
		mDist := m.VecDistFromVec(q)

		h.Link(&Item{id: m.id, dist: mDist}, h.Nodes[h.EntryNodeId], m.level)

		n := NewNode(2, []float32{6.1, 6.1}, 4)
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

func TestSpawnLevelDistribution(t *testing.T) {
	t.Run("plot distribution", func(t *testing.T) {
		h := NewHNSW(2, 12, 4, []float32{0, 0})

		levels := make(map[int]int)

		for i := 0; i < 1000; i++ {
			sLevel := h.spawnLevel()

			if _, ok := levels[sLevel]; ok {
				levels[sLevel] += 1
			} else {
				levels[sLevel] = 1
			}
		}

		numLevels := len(levels)

		if numLevels <= 1 {
			t.Fatalf("expected geometric distribution to increase to max layer")
		}

		prevCt := levels[numLevels-1]
		for level := numLevels - 2; level >= 1; level-- {
			currCt := levels[level]

			if prevCt > currCt {
				t.Fatalf("level %v has %v nodes. level %v has %v nodes.", level, currCt, level+1, prevCt)
			}

			prevCt = currCt
		}
	})

	t.Run("spawn nodes", func(t *testing.T) {
		h := NewHNSW(2, 12, 4, []float32{0, 0})

		levels := make(map[int]int)

		for i := 0; i < 1000; i++ {
			q := []float32{float32(i), float32(i + 1)}
			if err := h.Insert(q); err != nil {
				t.Fatal(err)
			}

			qNode := h.Nodes[h.NextNodeId-1]

			if !NearlyEqual(float64(qNode.VecDistFromVec(q)), 0) {
				t.Fatalf("expected qnode to have id %v, got different vector: %v", qNode.id, qNode.VecDistFromVec(q))
			}

			sLevel := qNode.level

			if _, ok := levels[sLevel]; ok {
				levels[sLevel] += 1
			} else {
				levels[sLevel] = 1
			}
		}

		numLevels := len(levels)

		if numLevels <= 1 {
			t.Fatalf("expected geometric distribution to increase to max layer")
		}

		prevCt := levels[numLevels-1]
		for level := numLevels - 2; level >= 1; level-- {
			currCt := levels[level]

			if prevCt > currCt {
				t.Fatalf("level %v has %v nodes. level %v has %v nodes.", level, currCt, level+1, prevCt)
			}

			prevCt = currCt
		}

		fmt.Printf("levels distribution: %v\n", levels)
	})
}

func TestHnsw_KnnCluster(t *testing.T) {

	var clusterC = []Vector{
		{0.2, 0.5},
		{0.2, 0.7},
		{0.3, 0.8},
		{0.5, 0.5},
		{0.4, 0.1},
	}

	var clusterCNodes = map[NodeId][]NodeId{
		1: {2, 4, 3, 5},
		2: {3, 1, 4, 5},
		3: {2, 1, 4, 5},
		4: {1, 2, 3, 5},
		5: {4, 1, 2, 3},
	}

	var clusterCVisited = map[NodeId][]bool{
		1: {false, true, true, true, true, true},
		2: {false, false, true, true, true, true},
		3: {false, false, false, true, true, true},
		4: {false, true, true, true, false, true},
		5: {false, true, true, true, true, false},
	}

	t.Run("cluster c insert", func(t *testing.T) {
		h := NewHNSW(2, 4, 4, []float32{0, 0})

		for i, q := range clusterC {
			if err := h.Insert(q); err != nil {
				t.Fatalf("failed to insert item %d: %v", i, err)
			}
		}

		fmt.Printf("%v", h.Nodes)

		if reflect.DeepEqual(h.Nodes, clusterCNodes) {
			t.Fatalf("expected all node keys to be the same as clusterC")
		}

		if len(h.Nodes) != 6 {
			t.Fatalf("expected 6 nodes, got %d", len(h.Nodes))
		}

		for i := 1; i <= 5; i++ {
			nodeId := NodeId(i)
			node := h.Nodes[nodeId]

			var nodeNN []NodeId
			visitedNN := make([]bool, 6) // counting entry

			for level := node.level; level >= 0; level-- {
				friendsAtLevel := node.friends[level]

				for !friendsAtLevel.IsEmpty() {
					peeled, err := friendsAtLevel.Peel()
					if err != nil {
						t.Fatal(err)
					}

					if !visitedNN[peeled.id] {
						nodeNN = append(nodeNN, peeled.id)
						visitedNN[peeled.id] = true
					}
				}
			}

			if reflect.DeepEqual(clusterCVisited[nodeId], visitedNN) {
				t.Fatalf("expected all node keys to be the same as clusterC")
			}
		}

	})

	var clusterA = []Vector{
		{0.2, 0.5},
		{0.2, 0.7},
		{0.3, 0.8},
		{0.5, 0.5},
		{0.4, 0.1},
		{0.3, 0.7},
		{0.27, 0.23},
		{0.12, 0.1},
		{0.23, 0.25},
		{0.3, 0.3},
		{0.01, 0.3},
	}

	var clusterB = []Vector{
		{4.2, 3.5},
		{4.2, 4.7},
		{4.3, 3.8},
		{4.5, 4.5},
		{4.4, 3.1},
		{4.3, 4.7},
		{4.27, 3.23},
		{4.1, 4.1},
		{4.12, 3.1},
		{4.23, 4.25},
		{4.3, 3.3},
		{4.01, 4.3},
	}

	t.Run("cluster a search", func(t *testing.T) {
		h := NewHNSW(2, 14, 8, []float32{0, 0})

		for i, q := range clusterA {
			if err := h.Insert(q); err != nil {
				t.Fatalf("failed to insert clusterA vector at iter %v, err: %v", i, err)
			}
		}

		for i, q := range clusterB {
			if err := h.Insert(q); err != nil {
				t.Fatalf("failed to insert clusterA vector at iter %v, err: %v", i, err)
			}
		}

		closest, err := h.KnnSearch([]float32{0.1, 0.1}, 4, 8)
		if err != nil {
			t.Fatalf("expected KNN search to return at least one KNN node. err: %v", err)
		}

		var clos []NodeId

		for !closest.IsEmpty() {
			peeled, err := closest.Peel()
			if err != nil {
				t.Fatal(err)
			}
			clos = append(clos, peeled.id)
		}

		if reflect.DeepEqual(clos, []NodeId{7, 0}) {
			t.Fatalf("got closest ids: %v, expected %v", clos, []NodeId{7, 0})
		}

  })
}
