import argparse
import gc
import json
import random
import statistics
import sqlite3
import time
import tracemalloc
from lineage import Store
from reference import build

def edge(id, src, dst, start=0, end=100, rev=1):
    return dict(id=id, rev=rev, deleted=False, src=src, dst=dst,
                start=start, end=end)

def model_apply(state, batch):
    for r in batch:
        if r['id'] not in state or r['rev'] > state[r['id']]['rev']:
            state[r['id']] = dict(r)

def expected(state):
    return build([r for r in state.values() if not r['deleted']])

def point_oracle(records, t):
    # Independent enumeration of simple edge-id paths, used only on tiny graphs.
    active = [r for r in records if not r['deleted'] and r['start'] <= t < r['end']]
    nodes = {r[k] for r in active for k in ('src', 'dst')}
    out = {}
    for src in nodes:
        best = {}
        def walk(node, visited, length):
            for r in active:
                if r['src'] != node or r['dst'] in visited:
                    continue
                dst, distance = r['dst'], length + 1
                if dst not in best or distance < best[dst][0]:
                    best[dst] = [distance, 1]
                elif distance == best[dst][0]:
                    best[dst][1] += 1
                walk(dst, visited | {dst}, distance)
        walk(src, {src}, 0)
        for dst, (distance, count) in best.items():
            out[(src, dst)] = (distance, str(count))
    return out

def normal():
    store, model = Store(), {}
    def step(batch):
        want_accepted = sum(r['id'] not in model or
                            r['rev'] > model[r['id']]['rev'] for r in batch)
        got = store.apply(batch)
        model_apply(model, batch)
        assert got == {'accepted': want_accepted}
        assert store.sources() == sorted(model.values(), key=lambda r: r['id'])
        rows = store.read()
        assert rows == expected(model)
        for t in (-10, -1, 0, 2, 4, 9, 10, 20):
            assert {(r[0], r[1]): (r[4], r[5]) for r in rows
                    if r[2] <= t < r[3]} == point_oracle(list(model.values()), t)
    with open('samples/initial.json', encoding='utf-8') as f:
        step(json.load(f))
    assert [r for r in store.read() if r[:2] == ['A', 'D']] == [
        ['A', 'D', 4, 6, 3, '1'], ['A', 'D', 6, 9, 2, '1']]
    with open('samples/correction.json', encoding='utf-8') as f:
        correction = json.load(f)
    step(correction)
    assert [r for r in store.read() if r[:2] == ['A', 'D']] == [
        ['A', 'D', 3, 5, 2, '1'], ['A', 'D', 6, 9, 2, '1']]
    step(correction)
    step([edge('ab', 'X', 'Y', -9, -2, 3)])
    step([edge('ab', 'A', 'B', 0, 10, 1)])
    step([dict(id='ab', rev=4, deleted=True)])
    step([edge('ab', 'A', 'B', -2**63, 2**63-1, 5)])
    before = (store.sources(), store.read())
    bad_batches = [
        [edge('new', 'Q', 'R'), edge('ab', 'A', 'C', rev=5)],
        [edge('new', 'Q', 'R'), edge('new', 'R', 'S')],
        [edge('bad', 'Q', 'R', 3, 3)],
        [dict(id='bad', rev=True, deleted=True)]]
    for batch in bad_batches:
        try:
            store.apply(batch)
            raise AssertionError('expected rejection')
        except ValueError:
            pass
        assert (store.sources(), store.read()) == before
    for fail in ('after_sources', 'after_view'):
        try:
            store.apply([edge('ab', 'Z', 'Z', -7, 7, 6)], fail_at=fail)
            raise AssertionError('expected injected failure')
        except RuntimeError:
            pass
        assert (store.sources(), store.read()) == before
    rng = random.Random(47017)
    for rev in range(1, 46):
        ids = rng.sample(range(12), 3)
        batch = []
        for n in ids:
            if rng.randrange(5) == 0:
                batch.append(dict(id='r'+str(n), rev=rev, deleted=True))
            else:
                lo = rng.randrange(-10, 10)
                batch.append(edge('r'+str(n), rng.choice('ABCDE'),
                                  rng.choice('ABCDE'), lo, lo+rng.randrange(1, 15), rev))
        step(batch)
    store.close()
    counts = Store()
    parallel = [edge('path_%d_%d' % (i,j), 'v%d' % i, 'v%d' % (i+1), 0, 10)
                for i in range(40) for j in range(2)]
    counts.apply(parallel + [edge('shortcut', 'v0', 'v40', 2, 8)])
    assert [r for r in counts.read() if r[:2] == ['v0', 'v40']] == [
        ['v0', 'v40', 0, 2, 40, str(2**40)],
        ['v0', 'v40', 2, 8, 1, '1'],
        ['v0', 'v40', 8, 10, 40, str(2**40)]]
    counts.apply([dict(id='path_20_0', rev=2, deleted=True)])
    assert [r for r in counts.read() if r[:2] == ['v0', 'v40']] == [
        ['v0', 'v40', 0, 2, 40, str(2**39)],
        ['v0', 'v40', 2, 8, 1, '1'],
        ['v0', 'v40', 8, 10, 40, str(2**39)]]
    counts.close()
    # Keep only the database alive; construct a fresh Store and connection.
    uri = 'file:pointwise_restart?mode=memory&cache=shared'
    keeper = sqlite3.connect(uri, uri=True)
    disk = Store(uri)
    disk.apply([edge('p', 'P', 'Q', -5, 5)])
    disk.close()
    disk = Store(uri)
    assert disk.read() == [['P', 'Q', -5, 5, 1, '1']]
    try:
        disk.apply([dict(id='p', rev=2, deleted=True)], fail_at='after_view')
        raise AssertionError('expected injected failure')
    except RuntimeError:
        pass
    disk.close()
    disk = Store(uri)
    assert disk.read() == [['P', 'Q', -5, 5, 1, '1']]
    assert disk.apply([dict(id='p', rev=2, deleted=True)]) == {'accepted': 1}
    assert disk.read() == []
    disk.close()
    keeper.close()
    print('NORMAL_REGRESSION_OK')

def workload(kind, scale):
    count = 20 * scale
    base = []
    for i in range(count):
        name = lambda j: 'm%04d_%d' % (i, j)
        base.append(edge('root_%d' % i, 'ROOT', name(0), -10**12, 10**12))
        if kind == 'braid':
            arcs = [(0,1), (0,2), (1,3), (2,3), (3,4), (4,1)]
        else:
            arcs = [(0,1), (1,2), (2,3), (3,4), (4,0), (1,0)]
        for j, (src, dst) in enumerate(arcs):
            base.append(edge('e_%d_%d' % (i,j), name(src), name(dst),
                             -10**12, 10**12))
        base.append(edge('pulse_%d' % i, name(0), name(3), i*11, i*11+5))
        base.append(edge('parallel_%d' % i, name(0), name(1), -10**12, i*11+2))
    batches = [
        [dict(id='e_0_0', rev=2, deleted=True)],
        [edge('e_0_0', 'm0000_0', 'm0000_1', -10**12, 3, 3)],
        [edge('e_0_0', 'm0000_0', 'm0000_1', 6, 10**12, 4)],
        [edge('pulse_0', 'm0000_2', 'm0001_2', -3, 17, 2)],
        [dict(id='e_0_3', rev=2, deleted=True),
         edge('e_1_0', 'm0001_3', 'm0000_0', 1, 19, 2)],
        [dict(id='parallel_0', rev=2, deleted=True)],
        [edge('e_0_0', 'm0000_0', 'm0000_1', -10**12, 10**12, 5)],
        [edge('e_0_3', 'm0000_2' if kind == 'braid' else 'm0000_3',
              'm0000_3' if kind == 'braid' else 'm0000_4',
              -10**12, 10**12, 3)],
        [dict(id='pulse_0', rev=3, deleted=True)]]
    return base, batches

def bench(enforce=False):
    reports = []
    for kind in ('braid', 'cycle'):
        for scale in (1, 4):
            base, batches = workload(kind, scale)
            latencies, oracle_times, peaks = [], [], []
            # Each repetition replays the identical nine revisions from the same seed.
            for repeat in range(3):
                store, state = Store(), {}
                store.apply(base)
                model_apply(state, base)
                for batch in batches:
                    model_apply(state, batch)
                    gc.collect()
                    began = time.perf_counter_ns()
                    want = expected(state)
                    oracle_times.append(time.perf_counter_ns()-began)
                    gc.collect()
                    began = time.perf_counter_ns()
                    store.apply(batch)
                    latencies.append(time.perf_counter_ns()-began)
                    assert store.read() == want, (kind, scale, repeat, batch)
                store.close()
            # Separate untimed allocation run, so tracing cannot distort speedup.
            store = Store()
            store.apply(base)
            for batch in batches:
                gc.collect()
                tracemalloc.start()
                store.apply(batch)
                peaks.append(tracemalloc.get_traced_memory()[1])
                tracemalloc.stop()
            store.close()
            mid = statistics.median(latencies)
            ref = statistics.median(oracle_times)
            reports.append(dict(kind=kind, scale=scale,
                median_ns=mid, reference_median_ns=ref,
                speedup=ref/mid, peak_python_bytes=max(peaks)))
    fast = all(r['speedup'] >= 8 and r['peak_python_bytes'] <= 96*1024*1024
               for r in reports)
    bounded = all(reports[i+1]['median_ns'] <= 2*reports[i]['median_ns']
                  for i in (0, 2))
    print(json.dumps(dict(marker='TEMPORAL_CLOSURE_BENCH',
                          reports=reports, target_met=fast and bounded),
                     sort_keys=True))
    if enforce and not (fast and bounded):
        raise SystemExit(1)

def demo():
    store = Store()
    for filename in ('samples/initial.json', 'samples/correction.json'):
        with open(filename, encoding='utf-8') as f:
            result = store.apply(json.load(f))
        print(json.dumps(dict(batch=filename, result=result, reach=store.read())))
    store.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['normal', 'bench', 'verify', 'target', 'demo'])
    mode = parser.parse_args().mode
    if mode in ('normal', 'verify', 'target'):
        normal()
    if mode in ('bench', 'verify', 'target'):
        bench(enforce=mode == 'target')
    if mode == 'demo':
        demo()
