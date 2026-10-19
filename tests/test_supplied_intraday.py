"""Optional operation originals, branch isolation, exact identities and real spawn."""
from dataclasses import replace
from decimal import Decimal, localcontext
from fractions import Fraction
import importlib.util
import unittest
from unittest.mock import patch

import equity_feature_contracts as c
from equity_feature_io_sdk import encode_result
from equity_feature_workers import BoundedSupervisor, CommandError, CommandErrorCode, ResourceBudget

AVAILABLE = importlib.util.find_spec('equity_features_intraday') is not None
if AVAILABLE:
    from equity_features_intraday import compute_intraday
    from equity_feature_workers.intraday import compute_intraday_inputs, intraday_headers, intraday_history, required_intraday_inputs
    from supplied_intraday_fixture import ALL, facts, work, values, split


@unittest.skipUnless(AVAILABLE, 'optional intraday extra absent in base environment')
class SuppliedIntraday(unittest.TestCase):
    def reject(self, code, task, batches):
        with self.assertRaises(CommandError) as caught:
            compute_intraday_inputs(task, batches)
        self.assertEqual(caught.exception.code, code)

    def calculate(self, context, config, batch, features=None):
        item = work(context, config, batch if type(batch) is tuple else (batch,), ALL if features is None else features)
        result = compute_intraday_inputs(item.task, item.batches)[0]
        self.assertEqual(result, compute_intraday(item.batches, context, config, features=tuple(h.feature_id for h in item.task.features)))
        return result

    def test_original_independent_eight_scalar_oracle(self):
        context, config, batch = facts()
        result = self.calculate(context, config, batch)
        expected = {'intraday.ema': float(Fraction(59, 4)), 'intraday.rsi': float(Fraction(1050, 13)), 'intraday.atr': float(Fraction(116, 27))}
        with localcontext() as ctx:
            ctx.prec = 350
            radius = 2 * (Decimal(2) / 3).sqrt()
            expected.update({'intraday.band_middle': 15., 'intraday.band_upper': float(15 + radius),
                'intraday.band_lower': float(15 - radius), 'intraday.band_width_ratio': float(2 * radius / 15),
                'intraday.band_position': float(Decimal('0.5') + 1 / (2 * radius))})
        self.assertEqual(len(result.values), 8)
        for col in result.values:
            self.assertAlmostEqual(col.values[0], expected[col.feature_id], delta=1e-12)
        self.assertTrue(all(q.status == c.Status.AVAILABLE for q in result.quality))
        self.assertEqual(result.metadata.backend_id, 'intraday.python')
        self.assertEqual(result.metadata.backend_version, '0.1.0a3')
        self.assertEqual(result.metadata.config_digest, config.digest)

    def test_all_nonempty_subsets_full_direct_parity(self):
        context, config, batch = facts()
        for mask in range(1, 1 << len(ALL)):
            selected = tuple(f for n, f in enumerate(ALL) if mask & (1 << n))
            self.calculate(context, config, batch, selected)

    def test_close_only_independent_from_atr(self):
        context, config, batch = facts()
        result = self.calculate(context, config, values(batch, high=None, low=None))
        for col, quality in zip(result.values, result.quality):
            if col.feature_id == 'intraday.atr':
                self.assertIsNone(col.values[0]); self.assertEqual(quality.status, c.Status.MISSING_INPUT)
            else:
                self.assertEqual(quality.status, c.Status.AVAILABLE)

    def test_target_close_absence_does_not_block_atr(self):
        context, config, batch = facts()
        result = self.calculate(context, config, values(batch, close=(10, 12, 11, 15, 14, None)))
        atr = next(col for col in result.values if col.feature_id == 'intraday.atr')
        self.assertAlmostEqual(atr.values[0], float(Fraction(116, 27)), delta=1e-12)
        self.assertEqual(next(q for q in result.quality if q.feature_id == 'intraday.atr').status, c.Status.AVAILABLE)
        self.assertTrue(all(q.status != c.Status.AVAILABLE for q in result.quality if q.feature_id != 'intraday.atr'))

    def test_old_gap_unknown_future_only_consuming_closure(self):
        context, config, batch = facts()
        for field, replacement in (('close', (10, None, 11, 15, 14, 16)), ('known_at_ns', (10, None, 30, 40, 50, 60)), ('known_at_ns', (10, 61, 30, 40, 50, 60))):
            result = self.calculate(context, config, values(batch, **{field: replacement}))
            for q in result.quality:
                self.assertEqual(q.status == c.Status.AVAILABLE, q.feature_id.startswith('intraday.band_'))

    def test_absent_empty_tuple_and_actual_empty_binding(self):
        context, config, batch = facts()
        absent = work(context, config, (), missing=True)
        with patch('equity_feature_workers.intraday.prepare_intraday', side_effect=AssertionError('absent must skip preparation')):
            missing = compute_intraday_inputs(absent.task, ())[0]
        self.assertTrue(all(q.status == c.Status.MISSING_INPUT and q.reasons == (c.Reason.ABSENT_INPUT,) for q in missing.quality))
        observed = work(context, config, ())
        empty = compute_intraday_inputs(observed.task, ())[0]
        self.assertEqual(empty, compute_intraday((), context, config, features=ALL))
        self.assertNotEqual(empty, missing)
        actual_empty = replace(batch, columns=tuple(c.Column(col.name, ()) for col in batch.columns),
            metadata=replace(batch.metadata, coverage=c.Coverage(0, 0, True), interval_coverage=()))
        delivered = self.calculate(context, config, actual_empty)
        self.assertEqual(len(delivered.metadata.inputs), 1)

    def test_multiple_chunks_and_original_order(self):
        context, config, batch = facts()
        chunks = split(batch)
        item = work(context, config, chunks)
        actual = compute_intraday_inputs(item.task, chunks)[0]
        self.assertEqual(actual, compute_intraday(chunks, context, config, features=ALL))
        self.reject(CommandErrorCode.RESULT, item.task, chunks[::-1])
        self.reject(CommandErrorCode.RESULT, item.task, chunks[:1])
        self.reject(CommandErrorCode.CONFIG, replace(item.task, inputs=item.task.inputs[::-1]), chunks[::-1])

    def test_actual_selected_warmup_and_reset_carry(self):
        context, config, batch = facts(sessions=True)
        bands = tuple(f for f in ALL if f.startswith('intraday.band_'))
        self.assertEqual(intraday_history(config, bands), (True, (), context.digest))
        self.assertEqual(intraday_history(config, ALL), (True, ('P',), context.digest))
        self.calculate(context, config, split(batch))
        reset_context, reset_config, reset_batch = facts(sessions=True, reset=True)
        self.assertEqual(intraday_history(reset_config, ALL), (True, (), reset_context.digest))
        result = self.calculate(reset_context, reset_config, split(reset_batch))
        self.assertEqual(next(col for col in result.values if col.feature_id == 'intraday.ema').values, (15.,))
        self.assertEqual(next(q for q in result.quality if q.feature_id == 'intraday.rsi').status, c.Status.INSUFFICIENT_HISTORY)

    def test_discovery_actual_fields_and_selection(self):
        _, config, _ = facts()
        close_only = required_intraday_inputs(config, ('intraday.ema', 'intraday.band_middle'))[0]
        atr = required_intraday_inputs(config, ('intraday.atr',))[0]
        self.assertNotIn('high', close_only.fields); self.assertNotIn('low', close_only.fields)
        self.assertIn('high', atr.fields); self.assertIn('target close unnecessary', atr.rule)
        self.assertEqual(tuple(h.feature_id for h in intraday_headers(ALL[::-1])), ALL)

    def test_closed_metadata_headers_context_and_missing_roles(self):
        context, config, batch = facts()
        item = work(context, config, (batch,))
        self.reject(CommandErrorCode.RESULT, replace(item.task, initialization_sha256='0' * 64), (batch,))
        self.reject(CommandErrorCode.RESULT, replace(item.task, features=item.task.features[::-1]), (batch,))
        broken = replace(config, parameters=tuple(replace(p, value='0' * 64) if p.name == 'context_sha256' else p for p in config.parameters))
        self.reject(CommandErrorCode.RESULT, replace(item.task, config=broken), (batch,))
        marker = work(context, config, (), missing=True).task.inputs[0]
        self.reject(CommandErrorCode.CONFIG, replace(item.task, inputs=item.task.inputs + (marker,)), (batch,))

    def test_constant_position_only_not_applicable(self):
        context, config, batch = facts()
        result = self.calculate(context, config, values(batch, close=(10,) * 6, high=(12,) * 6, low=(8,) * 6))
        qualities = {q.feature_id: q for q in result.quality}
        self.assertEqual(qualities['intraday.band_position'].status, c.Status.NOT_APPLICABLE)
        self.assertTrue(all(q.status == c.Status.AVAILABLE for f, q in qualities.items() if f.startswith('intraday.band_') and f != 'intraday.band_position'))

    def test_independent_symbols_sequential_thread_spawn(self):
        pairs = [facts(symbol) for symbol in ('A', 'B', 'C', 'D')]
        items = tuple(work(context, config, (batch,), name=context.instrument_id) for context, config, batch in pairs)
        expected = {item.task.task_sha256: encode_result(compute_intraday(item.batches, context, config, features=ALL)) for item, (context, config, _) in zip(items, pairs)}
        for mode in ('sequential', 'thread', 'process'):
            degree = 1 if mode == 'sequential' else 2
            outcome = BoundedSupervisor(mode=mode, budget=ResourceBudget(workers=degree, max_cpu_slots=degree + 1,
                max_in_flight=degree, max_resident_bytes=134217728)).run(items)
            self.assertEqual(len(outcome.tasks), 4)
            for execution in outcome.tasks:
                self.assertIsNone(execution.reason)
                self.assertEqual(encode_result(execution.results[0]), expected[execution.task.task_sha256])


if __name__ == '__main__':
    unittest.main()
