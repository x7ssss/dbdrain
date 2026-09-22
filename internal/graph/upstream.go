package graph

// UpstreamClosure computes the minimal strict upstream ancestor closure of startTables in the dependency graph.
// It:
// 1. Performs strict upward-only recursive DAG traversal along OutEdges (child -> parent).
// 2. Prunes all sibling and child down-traversals.
// 3. Maintains path-based cycle detection using an identity path array (visited_path) to prevent circular parent loops.
// 4. Supports multi-path convergence: computes the union of reachable ancestors without Cartesian duplication.
func (g *Graph) UpstreamClosure(startTables []string, isEdgeAllowed func(fromTable, toTable string) bool) []string {
	ancestors := make(map[string]bool)
	var ordered []string

	var traverse func(current string, visitedPath []string)
	traverse = func(current string, visitedPath []string) {
		for _, edge := range g.OutEdges[current] {
			parent := edge.ToTable
			if isEdgeAllowed != nil && !isEdgeAllowed(current, parent) {
				continue
			}
			// Path-based cycle detection: if parent is already in visitedPath, stop circular loop.
			if containsString(visitedPath, parent) {
				continue
			}

			if !ancestors[parent] {
				ancestors[parent] = true
				ordered = append(ordered, parent)
			}

			// Continue upward traversal along this path
			traverse(parent, append(visitedPath, parent))
		}

		// Also check virtual edges
		for _, ve := range g.VirtualEdges[current] {
			for _, tc := range ve.Mappings {
				parent := tc.Table
				if isEdgeAllowed != nil && !isEdgeAllowed(current, parent) {
					continue
				}
				if containsString(visitedPath, parent) {
					continue
				}
				if !ancestors[parent] {
					ancestors[parent] = true
					ordered = append(ordered, parent)
				}
				traverse(parent, append(visitedPath, parent))
			}
		}
	}

	for _, start := range startTables {
		if !ancestors[start] {
			ancestors[start] = true
			ordered = append(ordered, start)
		}
		traverse(start, []string{start})
	}

	return ordered
}

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}
