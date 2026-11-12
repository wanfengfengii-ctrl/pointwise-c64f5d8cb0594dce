"""Independent differential fuzzing of the incremental materializer."""
import random
import sqlite3
import sys

sys.path.insert(0, '/workspace')

from lineage import Store
from reference import build

if hasattr(sys, 'set_int_max_str_digits'):
    sys.set_int_max_str_digits(0)

def model_apply(state, batch):
    for r in batch:
        if r['id'] not in state or r['rev'] > state[r['id']]['rev']:
            state[r['id']] = dict(r)

def expected(state):
    return build([r for r in state.values() if not r['deleted']])

def fuzz(seed, uri=None):
    rng = random.Random(seed)
    state = {}
    store = Store(uri) if uri else Store()
    revs = {}
    ids = ['e%d' % i for i in range(rng.randrange(4, 16))]
    nodes = ['n%d' % i for i in range(rng.randrange(3, 9))]
    for step in range(120):
        # Occasionally reopen the database at a batch boundary.
        if uri and step % 23 == 22:
            store.close()
            store = Store(uri)
            # Resync the local revision clock with the persisted state.
            revs = {r['id']: r['rev'] for r in store.sources()}
        batch_size = rng.randrange(1, 4)
        batch = []
        used = set()
        for _ in range(batch_size):
            eid = rng.choice(ids)
            if eid in used:
                continue
            used.add(eid)
            rev = revs.get(eid, 0) + 1
            revs[eid] = rev
            if rng.randrange(6) == 0:
                batch.append(dict(id=eid, rev=rev, deleted=True))
            else:
                lo = rng.choice([-2**63, -50, -10, 0, 3, 7, 100])
                span = rng.choice([1, 4, 8, 2**63 - 1 - lo]) if lo != -2**63 \
                    else rng.choice([1, 5, 10**12, 2**63 - 1 - lo])
                hi = min(lo + span, 2**63 - 1)
                batch.append(dict(id=eid, rev=rev, deleted=False,
                                  src=rng.choice(nodes), dst=rng.choice(nodes),
                                  start=lo, end=hi))
        # 15% of batches include a same-rev conflicting record.
        if rng.randrange(7) == 0 and state:
            choices = [i for i in state if i not in used
                       and not state[i]['deleted']]
            if choices:
                eid = rng.choice(choices)
                conflict = dict(state[eid])
                conflict['src'] = nodes[0]
                batch.append(conflict)
        before_src = store.sources()
        before_read = store.read()
        try:
            got = store.apply(batch)
        except ValueError as e:
            assert str(e) in ('REVISION_CONFLICT', 'INVALID_BATCH',
                              'INVALID_RECORD', 'DUPLICATE_ID',
                              'INVALID_REV', 'INVALID_DELETED',
                              'INVALID_NODE', 'INVALID_TIME',
                              'INVALID_FIELDS', 'INVALID_INTERVAL'), \
                (seed, step, e)
            assert store.sources() == before_src
            assert store.read() == before_read
            continue
        model_apply(state, batch)
        assert store.sources() == sorted(state.values(), key=lambda r: r['id'])
        want = expected(state)
        got_rows = store.read()
        assert got_rows == want, (seed, step, batch,
                                  [r for r in got_rows if r not in want][:5],
                                  [r for r in want if r not in got_rows][:5])
        # Replay must be a no-op.
        store.apply(batch)
        assert store.read() == want
        # Injected failures must leave persistent state unchanged.
        if step % 17 == 5:
            probe_id = ids[0]
            probe_rev = revs.get(probe_id, 0) + 1
            probe = [dict(id=probe_id, rev=probe_rev, deleted=True)]
            snap_src, snap_read = store.sources(), store.read()
            for fail in ('after_sources', 'after_view'):
                try:
                    store.apply(probe, fail_at=fail)
                    raise AssertionError('injected failure missing')
                except RuntimeError:
                    pass
                assert store.sources() == snap_src
                assert store.read() == snap_read
            # Retry succeeds on the same connection; for a persistent database
            # also verify through a freshly opened connection.
            assert store.apply(probe) == {'accepted': 1}
            state[probe_id] = dict(id=probe_id, rev=probe_rev, deleted=True)
            revs[probe_id] = probe_rev
            want = expected(state)
            assert store.read() == want
            assert store.sources() == sorted(state.values(),
                                              key=lambda r: r['id'])
            if uri:
                store.close()
                store = Store(uri)
                assert store.read() == want
                assert store.sources() == sorted(
                    state.values(), key=lambda r: r['id'])
                assert store.apply(probe) == {'accepted': 0}
    store.close()

for seed in range(24):
    fuzz(seed)
# Every persistent run gets its own shared-cache database: closing the last
# connection releases a shared in-memory database.
keepers = []
for seed in range(6):
    path = 'file:fuzz_lineage_%d?mode=memory&cache=shared' % seed
    keepers.append(sqlite3.connect(path, uri=True))
    fuzz(1000 + seed, path)
for keeper in keepers:
    keeper.close()
print('FUZZ_OK')
