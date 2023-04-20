package topology

func Linear(nodes []string) map[string][]string {
	topology := make(map[string][]string)
	for i, n := range nodes {
		next := (i + 1) % len(nodes)
		topology[n] = []string{nodes[next]}
	}

	return topology
}
