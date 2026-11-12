import argparse
import json
import sqlite3
import sys
from collections import defaultdict, deque

if hasattr(sys, 'set_int_max_str_digits'):
    sys.set_int_max_str_digits(0)

def canonical(record):
    return json.dumps(record, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False)

def text_ok(value):
    if not isinstance(value, str) or not value:
        return False
    try:
        value.encode('utf-8')
    except UnicodeEncodeError:
        return False
    return True

def validate(records):
    if not isinstance(records, list):
        raise ValueError('INVALID_BATCH')
    seen = set()
    for r in records:
        if not isinstance(r, dict) or not text_ok(r.get('id')):
            raise ValueError('INVALID_RECORD')
        if r['id'] in seen:
            raise ValueError('DUPLICATE_ID')
        seen.add(r['id'])
        if type(r.get('rev')) is not int or not 1 <= r['rev'] < 2**63:
            raise ValueError('INVALID_REV')
        if type(r.get('deleted')) is not bool:
            raise ValueError('INVALID_DELETED')
        fields = {'id', 'rev', 'deleted'}
        if not r['deleted']:
            fields |= {'src', 'dst', 'start', 'end'}
            if any(not text_ok(r.get(k)) for k in ('src', 'dst')):
                raise ValueError('INVALID_NODE')
            if any(type(r.get(k)) is not int or not -2**63 <= r[k] < 2**63
                   for k in ('start', 'end')):
                raise ValueError('INVALID_TIME')
            if r['start'] >= r['end']:
                raise ValueError('INVALID_INTERVAL')
        if set(r) != fields:
            raise ValueError('INVALID_FIELDS')
    return records

class Store:
    def __init__(self, path=':memory:'):
        self.db = sqlite3.connect(path, isolation_level=None, uri=True)
        self.db.executescript("""
        PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS source(
            id TEXT PRIMARY KEY, rev INTEGER NOT NULL, body TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS reach(
            src TEXT NOT NULL, dst TEXT NOT NULL,
            start INTEGER NOT NULL, end INTEGER NOT NULL,
            hops INTEGER NOT NULL, paths TEXT NOT NULL,
            PRIMARY KEY(src,dst,start));
        CREATE TABLE IF NOT EXISTS schema_meta(key TEXT PRIMARY KEY, value TEXT);
        """)
        has_edge = self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='edge'"
        ).fetchone()
        if not has_edge:
            # One-time migration of a database produced by the full-rebuild
            # materializer: reconstruct the auxiliary edge table from sources
            # and normalize legacy bodies to the compact canonical encoding.
            self.db.execute('BEGIN IMMEDIATE')
            try:
                self.db.execute('CREATE TABLE edge('
                                'id TEXT PRIMARY KEY, src TEXT NOT NULL, '
                                'dst TEXT NOT NULL, start INTEGER NOT NULL, '
                                'end INTEGER NOT NULL)')
                for (body,) in self.db.execute('SELECT body FROM source'):
                    r = json.loads(body)
                    self.db.execute('UPDATE source SET body=? WHERE id=?',
                                    (canonical(r), r['id']))
                    if not r['deleted']:
                        self.db.execute('INSERT INTO edge VALUES(?,?,?,?,?)',
                                        (r['id'], r['src'], r['dst'],
                                         r['start'], r['end']))
                self.db.execute('COMMIT')
            except BaseException:
                self.db.execute('ROLLBACK')
                raise
        self.db.execute(
            "INSERT OR IGNORE INTO schema_meta VALUES('version','2')")
        # Volatile cache; purely an accelerator, rebuilt from the persisted
        # edge table after construction or any rolled-back transaction.
        self._edges = None
        self._fwd = None
        self._rev = None

    def close(self):
        self.db.close()

    def sources(self):
        return [json.loads(row[0]) for row in
                self.db.execute('SELECT body FROM source ORDER BY id')]

    def read(self):
        return [list(r) for r in self.db.execute(
            'SELECT src,dst,start,end,hops,paths FROM reach ORDER BY src,dst,start,end')]

    def _ensure_cache(self):
        if self._edges is not None:
            return
        edges, fwd, rev = {}, defaultdict(list), defaultdict(list)
        for eid, s, d, lo, hi in self.db.execute(
                'SELECT id,src,dst,start,end FROM edge'):
            edges[eid] = (s, d, lo, hi)
            fwd[s].append((d, eid))
            rev[d].append((s, eid))
        self._edges, self._fwd, self._rev = edges, fwd, rev

    def apply(self, records, fail_at=None):
        records = validate(records)
        self.db.execute('BEGIN IMMEDIATE')
        changes = []
        try:
            accepted = 0
            for r in records:
                old = self.db.execute('SELECT rev,body FROM source WHERE id=?',
                                      (r['id'],)).fetchone()
                body = canonical(r)
                if old and old[0] == r['rev'] and old[1] != body:
                    raise ValueError('REVISION_CONFLICT')
                if old and old[0] >= r['rev']:
                    continue
                self.db.execute('INSERT OR REPLACE INTO source VALUES(?,?,?)',
                                (r['id'], r['rev'], body))
                self._ensure_cache()
                old_edge = self._edges.get(r['id'])
                if r['deleted']:
                    new_edge = None
                else:
                    new_edge = (r['src'], r['dst'], r['start'], r['end'])
                if old_edge != new_edge:
                    changes.append((r['id'], old_edge, new_edge))
                accepted += 1
            if fail_at == 'after_sources':
                raise RuntimeError('INJECTED_AFTER_SOURCES')
            if accepted and changes:
                self._maintain(changes)
            if fail_at == 'after_view':
                raise RuntimeError('INJECTED_AFTER_VIEW')
            self.db.execute('COMMIT')
            return {'accepted': accepted}
        except BaseException:
            self.db.execute('ROLLBACK')
            # Discard any in-transaction cache mutation; reload from the
            # persisted edge table on the next call.
            self._edges = self._fwd = self._rev = None
            raise

    def _maintain(self, changes):
        """Incrementally refresh reach for one accepted batch.

        changes: list of (edge_id, old (src,dst,lo,hi)|None, new ...|None).
        Only node pairs that can witness a changed edge may change, and only
        inside the union U of every old/new edge interval; everything else is
        left byte-for-byte untouched.
        """
        self._ensure_cache()
        changed_ids = {c[0] for c in changes}

        # Union of old and new live edge states.
        uedge = dict(self._edges)
        nfwd, nrev = defaultdict(list), defaultdict(list)
        for eid, old, new in changes:
            if new is None:
                uedge.pop(eid, None)
            else:
                uedge[eid] = new
                nfwd[new[0]].append((new[1], eid))
                nrev[new[1]].append((new[0], eid))

        # Time region that can possibly change.
        lo = min(t[2] for _, old, new in changes
                 for t in (old, new) if t is not None)
        hi = max(t[3] for _, old, new in changes
                 for t in (old, new) if t is not None)

        # Union-graph adjacency: cached adjacency with changed ids replaced.
        fwd_memo, rev_memo = {}, {}
        def ufwd(u):
            if u not in fwd_memo:
                fwd_memo[u] = [(d, i) for d, i in self._fwd.get(u, ())
                               if i not in changed_ids] + list(nfwd.get(u, ()))
            return fwd_memo[u]
        def urev(u):
            if u not in rev_memo:
                rev_memo[u] = [(s, i) for s, i in self._rev.get(u, ())
                               if i not in changed_ids] + list(nrev.get(u, ()))
            return rev_memo[u]

        def reach_set(roots, succ):
            seen = set(roots)
            queue = deque(seen)
            while queue:
                u = queue.popleft()
                for v, _ in succ(u):
                    if v not in seen:
                        seen.add(v)
                        queue.append(v)
            return seen

        tails, heads = set(), set()
        for _, old, new in changes:
            if old is not None:
                tails.add(old[0]); heads.add(old[1])
            if new is not None:
                tails.add(new[0]); heads.add(new[1])
        # S*: vertices that can reach a changed tail; D*: vertices reachable
        # from a changed head.
        stars = reach_set(tails, urev)
        stard = reach_set(heads, ufwd)
        # Rv: vertices from which some D* vertex is reachable. Every vertex of
        # every S*->D* path lies in Rv, so edges leaving Rv can be ignored.
        rv = reach_set(stard, urev)

        # Relevant edges: potentially on an S*->D* path in the union graph.
        rel_ids = {eid for eid, (s, d, _, _) in uedge.items()
                   if s in rv and d in rv}

        # Slice boundaries inside U: only relevant edges can move an
        # annotation, so only their endpoints are needed (no unit expansion).
        points = {lo, hi}
        def add_points(interval):
            _, _, a, b = interval
            if lo < a < hi:
                points.add(a)
            if lo < b < hi:
                points.add(b)
        for _, old, new in changes:
            if old is not None:
                add_points(old)
            if new is not None:
                add_points(new)
        for eid in rel_ids - changed_ids:
            add_points(uedge[eid])
        points = sorted(points)

        pairs = sorted((s, d) for s in stars for d in stard if s != d)
        pair_set = set(pairs)

        # Existing rows of affected pairs: keep everything outside U and clip
        # overlapping rows at the U boundary. Every lookup uses the reach
        # primary key, so cost is proportional to the affected region rather
        # than the whole materialized closure.
        kept = defaultdict(list)
        for s, d in pairs:
            for a, b, h, p in self.db.execute(
                    'SELECT start,end,hops,paths FROM reach '
                    'WHERE src=? AND dst=?', (s, d)):
                key = (s, d)
                if b <= lo or a >= hi:
                    kept[key].append((a, b, h, p))
                else:
                    if a < lo:
                        kept[key].append((a, lo, h, p))
                    if b > hi:
                        kept[key].append((hi, b, h, p))

        def shortest(t):
            """BFS shortest hops/paths at time t over the new union graph."""
            found = {}
            for s in stars:
                dist = {s: 0}
                ways = {s: 1}
                queue = deque([s])
                while queue:
                    u = queue.popleft()
                    base = dist[u] + 1
                    for v, eid in ufwd(u):
                        if v not in rv:
                            continue
                        a, b = uedge[eid][2], uedge[eid][3]
                        if not (a <= t < b):
                            continue
                        if v not in dist:
                            dist[v] = base
                            ways[v] = ways[u]
                            queue.append(v)
                        elif dist[v] == base:
                            ways[v] += ways[u]
                for d in stard:
                    if d != s and d in dist:
                        found[(s, d)] = (dist[d], str(ways[d]))
            return found

        new_segments = defaultdict(list)
        for a, b in zip(points, points[1:]):
            for key, ann in shortest(a).items():
                new_segments[key].append((a, b) + ann)

        # Reassemble complete row lists for affected pairs only.
        rows = []
        for key in pair_set:
            segments = list(kept.get(key, ())) + list(new_segments.get(key, ()))
            segments = sorted(segments)
            merged = []
            for seg in segments:
                if merged and merged[-1][1] == seg[0] \
                        and merged[-1][2:] == seg[2:]:
                    merged[-1] = (merged[-1][0], seg[1], seg[2], seg[3])
                else:
                    merged.append(seg)
            for a, b, h, p in merged:
                rows.append((key[0], key[1], a, b, h, p))

        # Persist edge-state changes and swap closure rows atomically. Each
        # deletion hits the primary key; no full closure scan is allowed.
        for eid, old, new in changes:
            if new is None:
                self.db.execute('DELETE FROM edge WHERE id=?', (eid,))
            else:
                self.db.execute(
                    'INSERT OR REPLACE INTO edge VALUES(?,?,?,?,?)',
                    (eid, new[0], new[1], new[2], new[3]))
        self.db.executemany('DELETE FROM reach WHERE src=? AND dst=?', pairs)
        self.db.executemany('INSERT INTO reach VALUES(?,?,?,?,?,?)', rows)

        # Update the volatile cache to match the committed-to-be state.
        for eid, old, new in changes:
            if old is not None:
                s, d, _, _ = old
                self._fwd[s].remove((d, eid))
                self._rev[d].remove((s, eid))
                del self._edges[eid]
            if new is not None:
                s, d, _, _ = new
                self._fwd[s].append((d, eid))
                self._rev[d].append((s, eid))
                self._edges[eid] = new

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    sub = parser.add_subparsers(dest='command', required=True)
    apply = sub.add_parser('apply')
    apply.add_argument('batch')
    sub.add_parser('read')
    args = parser.parse_args()
    store = Store(args.db)
    try:
        if args.command == 'apply':
            with open(args.batch, encoding='utf-8') as f:
                data = json.load(f, parse_constant=lambda x: (_ for _ in ()).throw(
                    ValueError('INVALID_JSON')))
            result = store.apply(data)
        else:
            result = store.read()
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
    except (ValueError, RuntimeError) as e:
        print(json.dumps({'error': str(e)}))
        raise SystemExit(2)
    finally:
        store.close()

if __name__ == '__main__':
    main()
