import argparse
import json
import sqlite3
from reference import build

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
        """)
    def close(self):
        self.db.close()
    def sources(self):
        return [json.loads(row[0]) for row in
                self.db.execute('SELECT body FROM source ORDER BY id')]
    def read(self):
        return [list(r) for r in self.db.execute(
            'SELECT src,dst,start,end,hops,paths FROM reach ORDER BY src,dst,start,end')]
    def apply(self, records, fail_at=None):
        records = validate(records)
        self.db.execute('BEGIN IMMEDIATE')
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
                accepted += 1
            if fail_at == 'after_sources':
                raise RuntimeError('INJECTED_AFTER_SOURCES')
            if accepted:
                rows = build([r for r in self.sources() if not r['deleted']])
                self.db.execute('DELETE FROM reach')
                self.db.executemany('INSERT INTO reach VALUES(?,?,?,?,?,?)', rows)
            if fail_at == 'after_view':
                raise RuntimeError('INJECTED_AFTER_VIEW')
            self.db.execute('COMMIT')
            return {'accepted': accepted}
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

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
