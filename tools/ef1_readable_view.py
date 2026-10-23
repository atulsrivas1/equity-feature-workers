"""Readable per-case view of one EF1 demo example page.

Reads the canonical result JSON already embedded in the example's own HTML page and
only reformats it: no value is recomputed. Local demonstration only; inputs are synthetic.
"""
from __future__ import annotations

from html import escape, unescape
import json
from pathlib import Path
import re

_STYLE = ('body{font:16px/1.45 system-ui,sans-serif;max-width:1100px;margin:1rem auto;padding:0 1rem}'
    '.wrap{overflow-x:auto;margin-bottom:1rem}table{border-collapse:collapse;width:100%}'
    'td,th{border:1px solid #888;padding:4px 8px;text-align:left;vertical-align:top}'
    'th{background:#eee;position:sticky;top:0}code{overflow-wrap:anywhere}small{display:block;color:#555}'
    '.null{color:#a00;font-style:italic}.note{background:#f6f6f6;border-left:4px solid #888;padding:.3rem 1rem}')


def load_results(page: Path) -> tuple[list[dict], dict]:
    """The canonical result objects (one per line of the page's block, one per run) and the printed facts."""
    text = page.read_text(encoding='utf-8')
    blocks = re.findall(r'<pre>(.*?)</pre>', text, re.S)
    if len(blocks) != 1:
        raise ValueError('expected exactly one canonical result block in ' + page.name)
    task = re.search(r'Task ([0-9a-f]{64}); content ([0-9a-f]{64})', text)
    title = re.search(r'<h1>(.*?)</h1>', text)
    facts = dict(task=task.group(1) if task else None, content=task.group(2) if task else None,
        title=unescape(title.group(1)) if title else '')
    lines = [line for line in unescape(blocks[0]).splitlines() if line.strip()]
    if not lines:
        raise ValueError('empty canonical result block in ' + page.name)
    return [json.loads(line)['fields'] for line in lines], facts


def plain(value):
    """Kind and raw value of a canonical scalar (null, bool, string or a one-key tagged object)."""
    if value is None:
        return 'null', None
    if isinstance(value, bool):
        return 'bool', value
    if isinstance(value, str):
        return 'string', value
    if isinstance(value, dict) and set(value) == {'enum', 'value'}:
        return 'enum', value['value']
    if isinstance(value, dict) and len(value) == 1:
        (kind, raw), = value.items()
        return kind, raw
    raise ValueError('unrecognised canonical scalar: ' + json.dumps(value))


def show_value(dtype: str, value) -> str:
    """HTML for one stored value, exact as stored."""
    kind, raw = plain(value)
    if kind == 'null':
        return '<span class="null">null</span>'
    if kind == 'float64':
        number = float.fromhex(raw)
        small = raw if number.hex() == raw else raw + ' (stored text)'
        return '<code>' + escape(repr(number)) + '</code><small>' + escape(small) + '</small>'
    if kind == 'int' and dtype == 'decimal128':
        return '<code>' + escape(raw) + '</code><small>exact coefficient</small>'
    if kind == 'bool':
        return '<code>' + ('true' if raw else 'false') + '</code>'
    return '<code>' + escape(str(raw)) + '</code>'


def entity_text(entity: dict) -> str:
    return ', '.join(key + '=' + str(item) for key, item in sorted(entity['fields'].items()))


def count(item) -> str:
    """Text of an optional tagged integer; null when absent."""
    return 'null' if item is None else str(plain(item)[1])


def table(head: tuple[str, ...], rows: list[list[str]]) -> str:
    return ('<div class="wrap"><table><tr>' + ''.join('<th>' + escape(name) + '</th>' for name in head)
        + '</tr>' + ''.join('<tr>' + ''.join('<td>' + cell + '</td>' for cell in row) + '</tr>' for row in rows)
        + '</table></div>')


def feature_rows(result: dict) -> list[dict]:
    """One row per (feature, entity); quality rows are matched by feature and entity."""
    quality = {(row['fields']['feature_id'], entity_text(row['fields']['entity'])): row['fields']
        for row in result['quality']}
    rows = []
    for column in result['values']:
        fields = column['fields']
        for entity, value in zip(fields['entities'], fields['values'], strict=True):
            key = (fields['feature_id'], entity_text(entity))
            rows.append(dict(feature=fields['feature_id'], entity=key[1], dtype=fields['dtype']['value'],
                unit=fields['unit'], value=value, quality=quality[key]))
    return rows


def summary(result: dict, limit: int = 4) -> str:
    """Compact feature=value text for the index; plain text, escaped by the caller."""
    rows = feature_rows(result)
    parts = []
    for row in rows[:limit]:
        kind, raw = plain(row['value'])
        shown = 'null' if kind == 'null' else repr(float.fromhex(raw)) if kind == 'float64' else str(raw)
        parts.append(row['feature'].rsplit('.', 1)[-1] + '=' + shown)
    return '; '.join(parts) + (' ... (%d more)' % (len(rows) - limit) if len(rows) > limit else '')


def run_labels(results: list[dict]) -> list[str]:
    """Label per run of a multi-run case: prefix length (bars observed in the stored input coverage) and series.

    The example runs series A then B at each prefix length, so the series is the order among runs of one length.
    """
    seen: dict[int, int] = {}
    labels = []
    for result in results:
        ids = sorted({row['feature'] for row in feature_rows(result)})
        if ids and all(i.startswith(('breadth.', 'adaptive.')) for i in ids):
            # EF3 breadth and adaptive runs select outputs, not prefixes: label by the selected output or the joint run.
            labels.append(ids[0] if len(ids) == 1 else 'joint')
            continue
        length = sum(int(plain(entry['fields']['metadata']['fields']['coverage']['fields']['observed'])[1])
            for entry in result['metadata']['fields']['inputs'])
        seen[length] = seen.get(length, 0) + 1
        labels.append('prefix length %d, series %s' % (length, 'AB'[seen[length] - 1] if seen[length] <= 2 else str(seen[length])))
    return labels


def run_tables(result: dict, level: int) -> str:
    """Features and evidence sections of one stored result, headings at the given level."""
    features = []
    for item in feature_rows(result):
        quality = item['quality']
        features.append(['<code>' + escape(item['feature']) + '</code>', escape(item['entity']),
            show_value(item['dtype'], item['value']), escape(item['unit']), escape(item['dtype']),
            escape(plain(quality['status'])[1]), escape(', '.join(str(plain(reason)[1]) for reason in quality['reasons']) or '-'),
            escape(count(quality['expected']) + ' / ' + count(quality['observed']))])
    evidence = []
    for item in (entry['fields'] for entry in result['evidence']):
        evidence.append([escape(item['feature_id']), escape(item['input_id']), escape(item['row_id']),
            escape(count(item['event_ns'])), escape(count(item['known_at_ns'])),
            escape(count(item['effective_start_ns']) + ' to ' + count(item['effective_end_ns'])),
            escape(item['use']), escape(item['boundary']), escape(entity_text(item['entity']))])
    return ('<h%d>Features</h%d>' % (level, level) + table(('Feature', 'Entity', 'Value', 'Unit', 'Dtype', 'Status',
            'Reasons', 'Quality expected / observed'), features)
        + '<h%d>Evidence (' % level + str(len(evidence)) + ')</h%d>' % level + (table(('Feature', 'Input', 'Row id',
            'event_ns', 'known_at_ns', 'Effective start to end', 'Use', 'Boundary', 'Entity'), evidence)
            if evidence else '<p>No evidence rows.</p>'))


def render_view(row: dict, results: list[dict], facts: dict, gaps: tuple[str, ...]) -> str:
    """Whole readable page for one case; row is the runner's record of the run."""
    flags = row['flags'] or {}
    shown = ', '.join(key + '=' + str(item) for key, item in flags.items() if isinstance(item, (bool, str, int)))
    if len(results) == 1:
        sections = run_tables(results[0], 2)
    else:
        sections = ('<p>This case produced %d runs (stored results); one section per run follows.</p>' % len(results)
            + ''.join('<h2>Run %d of %d: %s</h2>' % (number, len(results), escape(label)) + run_tables(result, 3)
                for number, (label, result) in enumerate(zip(run_labels(results), results), 1)))
    return ('<!doctype html><html lang="en"><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<title>' + escape(row['example'] + ' ' + row['label']) + ' readable view</title><style>' + _STYLE + '</style>'
        '<h1>' + escape(facts['title'] or row['example']) + '</h1>'
        '<p><strong>Synthetic data, no unit tests run.</strong> Example <code>' + escape(row['example'])
        + '</code>, case <code>' + escape(row['label']) + '</code>, exit ' + str(row['code'])
        + ', ' + escape(row['status']) + '. Printed flags: ' + escape(shown or '-') + '.</p>'
        '<p>Task <code>' + escape(facts['task'] or 'not printed') + '</code><br>Content sha256 <code>'
        + escape(facts['content'] or 'not printed') + '</code></p>'
        '<p><a href="../' + escape(row['page']) + '">Raw example page (canonical JSON)</a> | DuckDB file <code>'
        + escape(row['database'] or '-') + '</code> (in the output directory)</p>'
        '<div class="note"><strong>How to read this.</strong> Each row of Features is one stored value. Floats show their '
        'exact repr with the stored hex text below; decimals show the exact integer coefficient; a null shows its status '
        'and reasons. Quality is expected / observed count. Evidence lists the input rows the value consumed or excluded; '
        'times are integer nanoseconds as stored. Values are copied from the stored canonical result, never recomputed.</div>'
        + sections
        + '<h2>Remaining gaps</h2><ul>' + ''.join('<li>' + escape(gap) + '</li>' for gap in gaps)
        + '</ul><p>No JavaScript or remote assets.</p></html>')


def write_view(row: dict, out: Path, gaps: tuple[str, ...]) -> dict:
    """Write view/<stem>.html for a run that produced a page; returns the link and summary text."""
    if row['code'] != 0 or not row['page']:
        return dict(view=None, summary=None)
    results, facts = load_results(out / row['page'])
    name = Path(row['page']).stem + '.html'
    (out / 'view').mkdir(exist_ok=True)
    (out / 'view' / name).write_text(render_view(row, results, facts, gaps), encoding='utf-8', newline='\n')
    return dict(view='view/' + name, summary=summary(results[0]) if len(results) == 1 else '%d runs (%s)' % (len(results), '; '.join(run_labels(results))))
