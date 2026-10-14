"""EQ088 finite native diagnostics. Explicit child entry; never a calculation API."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import time


def sort_oracle(rows, count):
    seen = bytearray(count)
    previous = None
    observed = 0
    for batch in rows:
        for identifier, payload in batch:
            assert type(identifier) is int and 0 <= identifier < count
            assert not seen[identifier], 'duplicate sorted ID'
            expected = hashlib.md5(str(identifier).encode()).hexdigest() + hashlib.md5(str(identifier + count).encode()).hexdigest()
            assert payload == expected, 'incorrect deterministic payload'
            key = (payload, identifier)
            assert previous is None or previous <= key, 'incorrect sort order'
            previous = key
            seen[identifier] = 1
            observed += 1
    assert observed == count and all(seen), 'incomplete sorted population'
    return dict(count=observed, exact_population_payload_order=True)


def public_profile(profile, root):
    """Profiles may contain the owned scratch path; preserve facts without local paths."""
    if isinstance(profile, dict):
        return {k: public_profile(v, root) for k, v in profile.items()}
    if isinstance(profile, list):
        return [public_profile(v, root) for v in profile]
    if isinstance(profile, str):
        return profile.replace(str(root), '<owned-root>').replace(root.as_posix(), '<owned-root>').replace('<owned-root>\\', '<owned-root>/')
    return profile


def profile_query(con, query, parameters, profile_path, consume):
    # SQL literal contains only our generated owned path, escaped as a literal.
    literal = str(profile_path).replace("'", "''")
    con.execute("SET profiling_output = '" + literal + "'")
    con.execute("SET enable_profiling = 'json'")
    start = time.perf_counter_ns()
    cursor = con.execute(query, parameters)
    value = consume(cursor)
    elapsed = time.perf_counter_ns() - start
    con.execute("SET enable_profiling = 'no_output'")
    profile = json.loads(profile_path.read_text(encoding='utf-8')) if profile_path.exists() else None
    return dict(elapsed_ns=elapsed, verified=value, profile=public_profile(profile, profile_path.parent))


def sort_case(root, count, memory, sink):
    import duckdb
    from equity_feature_duckdb_sink.schema import CONFIG
    temporary = root / 'temp'
    temporary.mkdir()
    config = dict(CONFIG) if sink else dict(threads='1', memory_limit=memory, max_temp_directory_size='512MiB',
        autoload_known_extensions='false', autoinstall_known_extensions='false')
    config['temp_directory'] = str(temporary)
    statement = 'SELECT i, md5(CAST(i AS VARCHAR)) || md5(CAST(i+? AS VARCHAR)) AS payload FROM range(?) t(i) ORDER BY payload,i'
    result = dict(kind='sort', count=count, policy='sink-zero-spill' if sink else 'source-spill-control',
                  memory=memory, settings={k: v for k, v in config.items() if k != 'temp_directory'}, sql=statement)
    start = time.perf_counter_ns()
    with duckdb.connect(':memory:', config=config) as con:
        def consume(cursor):
            def batches():
                while batch := cursor.fetchmany(4096):
                    yield batch
            return sort_oracle(batches(), count)
        try:
            result.update(profile_query(con, statement, [count, count], root / 'profile.json', consume))
            result['status'] = 'completed'
        except duckdb.OutOfMemoryException:
            result['status'] = 'capacity_denied'
            result['error_type'] = 'OutOfMemoryException'
    result['case_wall_ns'] = time.perf_counter_ns() - start
    result['remaining_temp_bytes'] = sum(p.stat().st_size for p in temporary.rglob('*') if p.is_file())
    return result


def expected_cell(identifier):
    return dict(result_ordinal=identifier, column_ordinal=0, entity_ordinal=0,
        namespace='synthetic-pilot', instrument_id=f'B{identifier % 64:04d}', session_id='S',
        feature_id='session.trade.count', algorithm_version='v1', dtype='int64', unit='trades',
        schema_version='1', is_null=False, value_int64=256, value_decimal128=None, value_float64=None,
        value_bool=None, value_utf8=None, value_wire=None, quality_status='available',
        quality_expected=256, quality_observed=256, quality_reasons=[])


def layout_case(root, count):
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    from equity_feature_parquet.projections import schemas
    assert count % 64 == 0
    schema, _ = schemas()
    base = [expected_cell(i) for i in range(count)]
    sorted_rows = sorted(base, key=lambda row: (row['instrument_id'], row['result_ordinal']))
    files, metadata = {}, {}
    for name, data, stats in [('A', base, False), ('B', sorted_rows, False), ('C', sorted_rows, True)]:
        path = root / (name + '.parquet')
        pq.write_table(pa.Table.from_pylist(data, schema=schema), path, version='2.6', compression='NONE',
            use_dictionary=False, write_statistics=stats, row_group_size=4096, write_batch_size=1024, data_page_size=65536, store_schema=True)
        actual = pq.read_table(path)
        assert actual.schema.equals(schema, check_metadata=True)
        assert sorted(actual.to_pylist(), key=lambda row: row['result_ordinal']) == base
        physical = pq.ParquetFile(path).metadata
        ranges = []
        for i in range(physical.num_row_groups):
            column = physical.row_group(i).column(schema.get_field_index('instrument_id'))
            bound = column.statistics
            assert (bound is not None) == stats
            ranges.append(dict(rows=physical.row_group(i).num_rows, minimum=None if bound is None else bound.min,
                               maximum=None if bound is None else bound.max))
        files[name] = path
        metadata[name] = dict(bytes=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            statistics=stats, row_groups=ranges, all_fields_and_schema_verified=True)
    records = []
    temporary = root / 'temp'
    temporary.mkdir()
    with duckdb.connect(':memory:', config=dict(threads='1', memory_limit='128MB',
            temp_directory=str(temporary), max_temp_directory_size='512MiB',
            autoload_known_extensions='false', autoinstall_known_extensions='false')) as con:
        for repeat in range(3):
            order = ['A', 'B', 'C'][repeat:] + ['A', 'B', 'C'][:repeat]
            for name in order:
                for selective in (False, True):
                    statement = 'SELECT count(*),sum(value_int64) FROM read_parquet(?)'
                    if selective:
                        statement += " WHERE instrument_id='B0017'"
                    expected = (count // 64, count * 4) if selective else (count, count * 256)
                    def consume(cursor):
                        values = cursor.fetchall()
                        assert values == [expected], (values, expected)
                        return list(expected)
                    record = profile_query(con, statement, [str(files[name])], root / f'{name}-{repeat}-{selective}.json', consume)
                    records.append(dict(layout=name, selective=selective, repeat=repeat, sql=statement, **record))
    return dict(kind='layout', logical_cells=count, metadata=metadata, records=records,
                authoritative_receipts_modified=False, production_layout_adopted=False)


def main():
    import duckdb
    settings = json.loads(sys.stdin.read())
    root = Path(settings['root']).resolve()
    root.mkdir(parents=True, exist_ok=False)
    result = sort_case(root, settings['count'], settings['memory'], settings['sink']) if settings['kind'] == 'sort' else layout_case(root, settings['count'])
    result['duckdb_version'] = duckdb.__version__
    result['owned_file_bytes_after'] = sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
    print(json.dumps(result, sort_keys=True, allow_nan=False))


if __name__ == '__main__':
    main()
