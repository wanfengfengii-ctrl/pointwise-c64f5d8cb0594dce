# Temporal shortest-lineage baseline

## Existing service and allowed scope
lineage.py is the existing Python 3 standard-library / SQLite materializer.
reference.py is the deliberately slow full-rebuild oracle.
checks.py and samples/ provide business regression, repeatable correction workloads and a real demo.
Optimize the existing materialization path; no new business endpoint is requested.
Keep reference.py, existing checks and sample expectations unchanged; additional regression files are allowed.
Do not dispatch by benchmark node names, workload kind, size, or test mode.
No external package or system service is required.

## Public entry points
python3 lineage.py --db PATH apply samples/initial.json
python3 lineage.py --db PATH apply samples/correction.json
python3 lineage.py --db PATH read
The database is created automatically. Each command writes exactly one JSON value to stdout.
apply accepts a UTF-8 JSON array and returns {"accepted":N}, counting accepted newer versions.
read returns [[src,dst,start,end,hops,paths],...] sorted by src,dst,start,end using Unicode code point ordering.
hops is the positive integer minimum edge count. paths is the exact count of shortest edge-id sequences,
encoded as a decimal string without a sign or leading zeroes. Parallel ids are distinct choices.
Errors currently represented by ValueError or injected RuntimeError return {"error":"MESSAGE"} and exit 2.
Preserve validation order and error messages in lineage.validate and Store.apply.
Store(path=':memory:') exposes apply(records, fail_at=None), read(), sources(), close(), and db.
sources() returns all latest source records, including tombstones, sorted by id.
SQLite file: URIs remain supported. Connections may be closed and reopened at any batch boundary.

## Source records and meaning
A live edge is {"id":"ab","rev":1,"deleted":false,"src":"A","dst":"B","start":0,"end":10}.
A tombstone is {"id":"ab","rev":2,"deleted":true}; no other fields are permitted.
id/src/dst are nonempty strings encodable as UTF-8 (no isolated surrogate code points). rev is an integer in [1,2**63); bool is not an integer here.
start/end are integers in [-2**63,2**63); require start < end.
A batch contains at most one record per id. The entire batch is validated before version checks.
Keep the highest rev per id, including tombstones. Ignore lower revisions.
Equal rev and equal canonical content is a no-op; equal rev and unequal content is REVISION_CONFLICT.
Canonical content is lineage.canonical(record). JSON object field order does not affect equality.
Newer revisions may move either endpoint, change the time interval, delete, or resurrect an edge.
Different ids can represent parallel edges. Self-edges and directed cycles are valid.
At any time t, use exactly the nondeleted edges satisfying start <= t < end.
For distinct nodes s,d, materialize times where at least one directed path from s to d exists,
annotated with the minimum hops and the exact number of paths achieving that minimum.
All edges of a witnessing path must be active at the same t. Shortest paths have no repeated vertex;
cycles neither create infinite counts nor justify preserving unsupported paths after a deletion.
Do not output s == d, including when a nonempty cycle exists.
For each ordered pair, output maximal nonempty half-open intervals on which both hops and paths are constant.
Touching intervals merge only when both annotations agree; unreachable times have no row.
Endpoint magnitudes must not cause expansion into unit timestamps or floating point conversion.

## Atomic state
source(id,rev,body) and reach(src,dst,start,end,hops,paths) retain their current public row meanings.
Extra persistent indexes or tables are allowed; they must survive restart and participate in the same transaction.
Each batch observes its final accepted source state, independent of record order.
All accepted revisions, closure changes and auxiliary state commit together or roll back together.
Keep fail_at='after_sources' and fail_at='after_view', raising the existing injected RuntimeError.
After either injected failure, persistent state must be unchanged; immediate retry and reopen-then-retry must agree.
Same-version replay, stale versions and empty batches do not change the closure.
Readers of another connection see a complete committed state, never half a batch.
Purely volatile caches must not be required for correctness after a fresh Store construction.

## Concrete example
After samples/initial.json, the A->D rows are exactly ["A","D",4,6,3,"1"] and ["A","D",6,9,2,"1"].
After samples/correction.json, they are exactly ["A","D",3,5,2,"1"] and ["A","D",6,9,2,"1"].
A->B in the second result includes [6,9) with hops=2, paths="1", because A->C->B survives.
checks.py demo prints both complete materialized results.

## Reproducible measurements and optimization target
python3 checks.py normal must exit 0 and print NORMAL_REGRESSION_OK.
python3 checks.py bench must exit 0, compare every measured state with reference.build,
and print TEMPORAL_CLOSURE_BENCH with actual timing, Python allocation and target_met values.
The correct unoptimized baseline has target_met=false; this is not a business failure.
The fixed braid/cycle workloads each use scales 1 and 4: 20 or 80 modules connected to ROOT.
Each module has parallel paths, cycles and time boundaries; the nine correction batches are identical across scales.
Both graphs are one weakly connected component; traversing that whole component is not a local optimization.
Each timing case is the median of 27 single-batch durations from three independently seeded repetitions.
Oracle time is full reference.build on final live edges in the same process; validation, source updates,
closure writes and commit ARE included in Store.apply time. Initial import, read comparison, oracle computation
and garbage collection are outside Store.apply time. Report absolute medians as well as the ratio.
An independent untimed replay uses tracemalloc around each apply; the peak is additional traced Python
allocation during that call, not RSS and not total retained SQLite/cache memory.
For every kind/scale case, correction median must be <= 1/8 of reference median and peak <= 96 MiB.
For each kind, scale-4 correction median must be <= 2 times scale-1 correction median.
python3 checks.py target enforces all those thresholds and the normal business regression.
Keep timing boundaries, repetitions, dataset and oracle unchanged; do not disable GC to alter the measurement.
The target must hold for ordinary run-to-run variation, not just a selected best run.
The correction path must not unconditionally scan all source rows, clear the closure, or recompute complete
global time slices. Empty-database import and one-time auxiliary-index construction may process the full input.
Affected-region traversal is permitted. Arbitrary legal graphs remain semantically supported.
Do not specialize behavior to the supplied fixtures; independent checks use other ids, graph shapes,
revision orders, time boundaries, conflicts, cycles, parallel edges and reopen/retry sequences.
Keep verify and demo meaningful in .pointwise-runtime.json; final verify must invoke checks.py target.
Deliver the implementation and a short explanation of dependency invalidation, measured speedup and restart behavior.
