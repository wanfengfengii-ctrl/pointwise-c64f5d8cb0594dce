"""Independent full temporal shortest-lineage oracle."""
from collections import defaultdict, deque
import sys

if hasattr(sys, 'set_int_max_str_digits'):
    sys.set_int_max_str_digits(0)

def build(edges):
    points = sorted({r[k] for r in edges for k in ('start', 'end')})
    spans = defaultdict(list)
    for lo, hi in zip(points, points[1:]):
        graph = defaultdict(list)
        for r in edges:
            if r['start'] <= lo < r['end']:
                graph[r['src']].append(r['dst'])
        for src in sorted(graph):
            distance, ways = {src: 0}, {src: 1}
            queue = deque([src])
            while queue:
                node = queue.popleft()
                for dst in graph.get(node, ()):
                    candidate = distance[node] + 1
                    if dst not in distance:
                        distance[dst], ways[dst] = candidate, 0
                        queue.append(dst)
                    if distance[dst] == candidate:
                        ways[dst] += ways[node]
            for dst in sorted(set(distance) - {src}):
                hops, count = distance[dst], str(ways[dst])
                parts = spans[(src, dst)]
                if parts and parts[-1][1] == lo and parts[-1][2:] == [hops, count]:
                    parts[-1][1] = hi
                else:
                    parts.append([lo, hi, hops, count])
    return sorted([src, dst] + part
                  for (src, dst), parts in spans.items() for part in parts)
