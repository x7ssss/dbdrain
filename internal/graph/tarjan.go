package graph

// SCC represents a strongly connected component (set of tables in a cycle).
type SCC struct {
	Tables []string
	HasCycle bool // true if len > 1 or self-referential
}

// TarjanSCC runs Tarjan's algorithm and returns SCCs in reverse topological order.
func TarjanSCC(g *Graph) []SCC {
	index := 0
	stack := []string{}
	onStack := map[string]bool{}
	indices := map[string]int{}
	lowlinks := map[string]int{}
	visited := map[string]bool{}
	var sccs []SCC

	var strongconnect func(v string)
	strongconnect = func(v string) {
		indices[v] = index
		lowlinks[v] = index
		index++
		stack = append(stack, v)
		onStack[v] = true
		visited[v] = true

		for _, edge := range g.OutEdges[v] {
			w := edge.ToTable
			if !visited[w] {
				strongconnect(w)
				if lowlinks[w] < lowlinks[v] {
					lowlinks[v] = lowlinks[w]
				}
			} else if onStack[w] {
				if indices[w] < lowlinks[v] {
					lowlinks[v] = indices[w]
				}
			}
		}

		if lowlinks[v] == indices[v] {
			var scc SCC
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				scc.Tables = append(scc.Tables, w)
				if w == v {
					break
				}
			}
			// Check for self-referential or multi-node cycle
			if len(scc.Tables) > 1 {
				scc.HasCycle = true
			} else {
				// Self-referential check
				for _, edge := range g.OutEdges[scc.Tables[0]] {
					if edge.ToTable == scc.Tables[0] {
						scc.HasCycle = true
						break
					}
				}
			}
			sccs = append(sccs, scc)
		}
	}

	for v := range g.Nodes {
		if !visited[v] {
			strongconnect(v)
		}
	}

	return sccs
}

// TopoOrder returns tables in topological order (dependencies first).
// SCCs with cycles are treated as a single unit.
func TopoOrder(sccs []SCC) []string {
	var order []string
	// Tarjan returns SCCs in reverse topological order (leaves first).
	// To get dependencies first, we iterate in the order they were found.
	for _, scc := range sccs {
		for _, t := range scc.Tables {
			order = append(order, t)
		}
	}
	return order
}
