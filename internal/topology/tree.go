package topology

func bfs(nodes []string, children int, topology map[string][]string, idx int, parent *int) {
	if idx >= len(nodes) {
		return
	}

	if parent != nil {
		topology[nodes[idx]] = append(topology[nodes[idx]], nodes[*parent])
		topology[nodes[*parent]] = append(topology[nodes[*parent]], nodes[idx])
	}

	for i := 1; i <= children; i++ {
		bfs(nodes, children, topology, (children*idx)+i, &idx)
	}
}

func Tree(nodes []string, children int) map[string][]string {
	// take node i
	// add next n to children
	// for each children add next n as children
	topology := make(map[string][]string)
	for _, n := range nodes {
		topology[n] = make([]string, 0)
	}

	bfs(nodes, children, topology, 0, nil)

	return topology
}
