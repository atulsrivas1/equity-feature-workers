"""Original hand-derived fixtures through supplied callbacks and real worker modes."""
from dataclasses import replace
import hashlib
import io
import pickle
import sys
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import equity_feature_contracts as c
from equity_feature_io_sdk import encode_result
from equity_feature_workers import BoundedSupervisor, CommandError, CommandErrorCode, InputManifest, TaskManifest, WorkItem, ResourceBudget
from equity_feature_workers.supplied_session import compute_supplied_session_inputs, supplied_headers, supplied_history, required_supplied_inputs
import supplied_original as original


def manifest(role, source):
    if source is None:
        return InputManifest(role, hashlib.sha256(('missing:' + role).encode()).hexdigest(), 'original-v1', 'missing', None)
    coverage = source.metadata.coverage
    state = 'empty' if coverage.complete and coverage.expected == 0 else 'complete' if coverage.complete else 'partial'
    return InputManifest(role, hashlib.sha256(pickle.dumps(source, protocol=5)).hexdigest(), 'original-v1', state, c.InputBinding(role, source.kind, source.metadata))


def work(family, config, primary, auxiliary=None, *, name='original'):
    primary_role = 'trades' if family == 'top_k' else 'quotes' if family == 'continuous_quotes' else 'bars'
    slots = [(primary_role, primary)]
    if family == 'bars_prior_close':
        slots.append(('prior_close', auxiliary))
    elif family == 'continuous_quotes' and next(p.value for p in config.parameters if p.name == 'initial_state') == 'seed':
        slots.append(('quote_seed', auxiliary))
    inputs = tuple(manifest(role, source) for role, source in slots)
    governed = (c.IntervalSpec('P', -30, 0), c.IntervalSpec('S', 0, config.session.close_ns))
    history = supplied_history(config, family, inputs, governed)
    task = TaskManifest('original-job', 'generation1', name, family, ('A',), config,
        supplied_headers(family), inputs, governed, 'original-destination', 'serialized_destination', 2, 1048576,
        history[0], history[1], history[2])
    return WorkItem(task, tuple(source for _, source in slots if source is not None), compute_supplied_session_inputs)


def original_work():
    items = []
    pure = []

    def wrapper(family, function):
        def calculate(batch, config, *, entity, **kwargs):
            item = work(family, config, batch, kwargs.get('prior_close', kwargs.get('seed')), name=f'case{len(items)}')
            expected = function(batch, config, entity=entity, **kwargs)
            actual = compute_supplied_session_inputs(item.task, item.batches)[0]
            assert actual == expected and encode_result(actual) == encode_result(expected)
            items.append(item); pure.append(expected)
            return actual
        return calculate

    original.CHECKS.clear(); original.RESULTS.clear()
    with patch.object(sys, 'argv', ['original']), redirect_stdout(io.StringIO()), \
         patch.object(original, 'compute_bars', wrapper('bars_prior_close', original.compute_bars)), \
         patch.object(original, 'compute_structure', wrapper('structure', original.compute_structure)), \
         patch.object(original, 'compute_top_k', wrapper('top_k', original.compute_top_k)), \
         patch.object(original, 'compute_time_weighted', wrapper('continuous_quotes', original.compute_time_weighted)):
        original.main()
    assert len(original.CHECKS) == 85 and len(items) == 8
    return tuple(items), tuple(pure)


class SuppliedSession(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.items, cls.expected = original_work()

    def reject(self, code, task, batches):
        with self.assertRaises(CommandError) as caught:
            compute_supplied_session_inputs(task, batches)
        self.assertEqual(caught.exception.code, code)

    def test_eight_original_full_results_and_quality_counts(self):
        for item, expected in zip(self.items, self.expected):
            actual = compute_supplied_session_inputs(item.task, item.batches)[0]
            self.assertEqual(actual, expected)
            self.assertEqual(encode_result(actual), encode_result(expected))
        self.assertEqual([(q.expected, q.observed) for q in self.expected[0].quality], [(3, 3)] * 13)
        partial = self.expected[3]
        self.assertEqual([(q.expected, q.observed) for q in partial.quality], [(2, 1), (2, 0)])
        self.assertEqual([e.row_id for e in self.expected[4].evidence], ['C', 'A'])
        self.assertEqual([(q.expected, q.observed) for r in self.expected[5:] for q in r.quality], [(2, 2)] * 3)

    def test_sequential_thread_process_full_result_parity(self):
        expected = {item.task.task_sha256: encode_result(result) for item, result in zip(self.items, self.expected)}
        for mode in ('sequential', 'thread', 'process'):
            with self.subTest(mode=mode):
                degree = 1 if mode == 'sequential' else 2
                outcome = BoundedSupervisor(mode=mode,
                    budget=ResourceBudget(workers=degree, max_cpu_slots=degree + 1, max_in_flight=degree,
                                          max_resident_bytes=134217728)).run(self.items)
                self.assertEqual(len(outcome.tasks), len(expected))
                for execution in outcome.tasks:
                    self.assertIsNone(execution.reason)
                    self.assertEqual(len(execution.results), 1)
                    self.assertEqual(encode_result(execution.results[0]), expected[execution.task.task_sha256])

    def test_headers_and_discovery_reuse_actual_kinds(self):
        for item in self.items:
            self.assertEqual(supplied_headers(item.task.family), item.task.features)
            requirements = required_supplied_inputs(item.task.config, item.task.family)
            roles = {r.role: r for r in requirements}
            self.assertIn('availability', roles)
            if item.task.family == 'bars_prior_close':
                self.assertEqual(roles['prior_close'].kind, c.DataKind.DAILY)
                self.assertIn('close', roles['prior_close'].fields)
            if item.task.family == 'continuous_quotes':
                self.assertEqual(roles['quotes'].sampling, 'continuous')

    def test_structure_does_not_discover_or_consume_actual_notional(self):
        item = self.items[2]
        requirements = required_supplied_inputs(item.task.config, 'structure')
        self.assertNotIn('actual_notional', next(r for r in requirements if r.role == 'bars').fields)
        self.assertIn('actual_notional', required_supplied_inputs(item.task.config, 'bars_prior_close')[0].fields)
        original_batch = item.batches[0]
        projected = replace(original_batch, columns=tuple(col for col in original_batch.columns if col.name != 'actual_notional'))
        projected_work = work('structure', item.task.config, projected)
        self.assertEqual(compute_supplied_session_inputs(projected_work.task, projected_work.batches),
                         compute_supplied_session_inputs(item.task, item.batches))

    def test_exact_bijection_unmatched_missing_duplicate_reordered(self):
        item = self.items[0]
        self.reject(CommandErrorCode.RESULT, item.task, item.batches[:1])
        self.reject(CommandErrorCode.RESULT, item.task, item.batches[::-1])
        self.reject(CommandErrorCode.LIMIT, item.task, item.batches + item.batches[:1])
        self.reject(CommandErrorCode.RESULT, replace(item.task, max_input_batches=3), item.batches + item.batches[:1])
        self.reject(CommandErrorCode.CONFIG, replace(item.task, inputs=item.task.inputs[::-1]), item.batches[::-1])

    def test_header_history_and_actual_prior_scope_identity(self):
        item = self.items[0]
        self.reject(CommandErrorCode.RESULT, replace(item.task, features=item.task.features[:1]), item.batches)
        self.reject(CommandErrorCode.RESULT, replace(item.task, initialization_sha256='0' * 64), item.batches)
        self.reject(CommandErrorCode.RESULT, replace(item.task, warmup_sessions=()), item.batches)
        bad_sessions = (c.IntervalSpec('P', -31, 0), item.task.governed_sessions[1])
        history = supplied_history(item.task.config, item.task.family, item.task.inputs, bad_sessions)
        bad = replace(item.task, governed_sessions=bad_sessions, initialization_sha256=history[2])
        self.reject(CommandErrorCode.RESULT, bad, item.batches)

    def test_missing_primary_retains_real_auxiliary_binding(self):
        template = self.items[0]
        item = work('bars_prior_close', template.task.config, None, template.batches[1])
        result = compute_supplied_session_inputs(item.task, item.batches)[0]
        self.assertEqual([b.role for b in result.metadata.inputs], ['prior_close'])
        self.assertTrue(all(q.status == c.Status.MISSING_INPUT for q in result.quality))

    def test_unknown_and_future_prior_only_gate_prior_outputs(self):
        template = self.items[0]
        for known, reason in ((None, c.Reason.UNKNOWN_AVAILABILITY), (31, c.Reason.FUTURE_KNOWLEDGE)):
            prior = replace(template.batches[1], columns=tuple(
                c.Column(col.name, (known,)) if col.name == 'known_at_ns' else col for col in template.batches[1].columns))
            item = work('bars_prior_close', template.task.config, template.batches[0], prior)
            actual = compute_supplied_session_inputs(item.task, item.batches)[0]
            direct = original.compute_bars(item.batches[0], item.task.config, entity=c.EntityKey('A', 'S'), prior_close=prior)
            self.assertEqual(encode_result(actual), encode_result(direct))
            for column, quality, baseline in zip(actual.values, actual.quality, self.expected[0].values):
                if column.feature_id.rsplit('.', 1)[1] in ('overnight_gap', 'close_close_return'):
                    self.assertEqual(column.values, (None,)); self.assertEqual(quality.status, c.Status.MISSING_INPUT)
                    self.assertEqual(quality.reasons, (reason,))
                else:
                    self.assertEqual(column, baseline); self.assertEqual(quality.status, c.Status.AVAILABLE)
            self.assertEqual([b.role for b in actual.metadata.inputs], ['bars', 'prior_close'])

    def test_missing_notional_does_not_gate_other_bar_outputs(self):
        template = self.items[0]
        bars = replace(template.batches[0], columns=tuple(col for col in template.batches[0].columns if col.name != 'actual_notional'))
        item = work('bars_prior_close', template.task.config, bars, template.batches[1])
        actual = compute_supplied_session_inputs(item.task, item.batches)[0]
        for column, quality, baseline in zip(actual.values, actual.quality, self.expected[0].values):
            if column.feature_id.endswith('.notional'):
                self.assertEqual(column.values, (None,)); self.assertEqual(quality.reasons, (c.Reason.ABSENT_INPUT,))
            else:
                self.assertEqual(column, baseline); self.assertEqual(quality.status, c.Status.AVAILABLE)

    def test_unavailable_seed_and_open_update_preserve_temporal_evidence(self):
        template = self.items[5]
        for known, reason in ((None, c.Reason.UNKNOWN_AVAILABILITY), (101, c.Reason.FUTURE_KNOWLEDGE)):
            seed = replace(template.batches[1], columns=tuple(
                c.Column(col.name, (known,)) if col.name == 'known_at_ns' else col for col in template.batches[1].columns))
            for at_open in (False, True):
                quotes = template.batches[0]
                if at_open:
                    quotes = replace(quotes, columns=tuple(c.Column(col.name, (0, 50))
                        if col.name in ('event_ns', 'known_at_ns') else col for col in quotes.columns))
                item = work('continuous_quotes', template.task.config, quotes, seed)
                actual = compute_supplied_session_inputs(item.task, item.batches)[0]
                direct = original.compute_time_weighted(quotes, item.task.config, entity=c.EntityKey('A', 'S'), seed=seed)
                self.assertEqual(encode_result(actual), encode_result(direct))
                value = actual.values[0].values[0]; quality = actual.quality[0]
                self.assertEqual(value.durations.total, 100)
                self.assertEqual(value.durations.unknown, 0 if at_open else 10)
                self.assertEqual(quality.status, c.Status.AVAILABLE if at_open else c.Status.INCOMPLETE_COVERAGE)
                self.assertEqual(quality.reasons, () if at_open else tuple(dict.fromkeys((c.Reason.UNKNOWN_AVAILABILITY, reason))))
                self.assertEqual((actual.evidence[0].use, actual.evidence[0].exclusion_reason), ('excluded', reason))
                self.assertEqual((value.mean_spread, value.mean_bps), (2., 10000/51) if at_open else (None, None))

    def test_locked_zero_and_no_valid_duration_remain_distinct(self):
        template = self.items[7]
        for side, status, reasons in ((102, c.Status.AVAILABLE, ()),
                (None, c.Status.NOT_APPLICABLE, (c.Reason.ZERO_DENOMINATOR,))):
            quotes = replace(template.batches[0], columns=tuple(c.Column(col.name, (side, side))
                if col.name in ('bid', 'ask') else col for col in template.batches[0].columns))
            item = work('continuous_quotes', template.task.config, quotes)
            actual = compute_supplied_session_inputs(item.task, item.batches)[0]
            self.assertEqual(actual.quality[0].status, status); self.assertEqual(actual.quality[0].reasons, reasons)
            value = actual.values[0].values[0]
            self.assertEqual(value.durations.total, 100)
            self.assertEqual((value.mean_spread, value.mean_bps), (0., 0.) if side is not None else (None, None))

    def test_sampled_quotes_are_not_relabelled_continuous(self):
        template = self.items[7]
        quotes = replace(template.batches[0], metadata=replace(template.batches[0].metadata, sampling='trade_snapshot'))
        item = work('continuous_quotes', template.task.config, quotes)
        self.reject(CommandErrorCode.CONFIG, item.task, item.batches)

    def test_all_missing_and_explicit_empty_delivery(self):
        item = self.items[1]
        missing = work('bars_prior_close', item.task.config, None)
        result = compute_supplied_session_inputs(missing.task, ())[0]
        self.assertEqual(result.metadata.inputs, ())
        self.assertTrue(all(q.status == c.Status.MISSING_INPUT for q in result.quality))
        batch = item.batches[0]
        empty = replace(batch, columns=tuple(c.Column(col.name, ()) for col in batch.columns),
            metadata=replace(batch.metadata, coverage=c.Coverage(0, 0, True), interval_coverage=()))
        observed = work('bars_prior_close', item.task.config, empty)
        actual = compute_supplied_session_inputs(observed.task, observed.batches)[0]
        self.assertEqual(len(actual.metadata.inputs), 1)
        self.assertEqual(actual.values[4].values, (0,))


if __name__ == '__main__':
    unittest.main()
