package fire

import "sort"

func (e *Engine) Clusters() [][]string {
	var out [][]string
	for _, f := range e.Fires() {
		out = append(out, f.Attributes["detection_keys"].([]string))
	}
	sortSets(out)
	return out
}

// ReferenceClusters recomputes the fires from scratch with an O(n²) pairwise
// check over the final state of every detection, as a cross-check of the
// incremental clustering. Late detections are excluded, as in the stream.
func (e *Engine) ReferenceClusters() [][]string {
	var dets []*detection
	for _, d := range e.dets {
		if d.active() {
			dets = append(dets, d)
		}
	}
	sort.Slice(dets, func(i, j int) bool { return dets[i].offset < dets[j].offset })
	parent := make([]int, len(dets))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(i int) int {
		if parent[i] != i {
			parent[i] = find(parent[i])
		}
		return parent[i]
	}
	for i := range dets {
		for j := i + 1; j < len(dets); j++ {
			gap := dets[i].at.Sub(dets[j].at)
			if gap < 0 {
				gap = -gap
			}
			if gap <= e.cfg.MaxGap && HaversineKM(dets[i].lat, dets[i].lon, dets[j].lat, dets[j].lon) <= e.cfg.MaxDistanceKM {
				parent[find(i)] = find(j)
			}
		}
	}
	groups := map[int][]string{}
	for i, d := range dets {
		groups[find(i)] = append(groups[find(i)], d.key)
	}
	var out [][]string
	for _, g := range groups {
		if len(g) >= e.cfg.MinDetections {
			sort.Strings(g)
			out = append(out, g)
		}
	}
	sortSets(out)
	return out
}

func sortSets(s [][]string) {
	sort.Slice(s, func(i, j int) bool { return s[i][0] < s[j][0] })
}
