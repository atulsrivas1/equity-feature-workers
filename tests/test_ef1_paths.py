"""Unexecuted independent literal paths plus closed worker/publication controls."""
from dataclasses import replace
from fractions import Fraction
import unittest
from unittest.mock import patch

import equity_feature_contracts as c
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import decode_result, encode_result
from equity_feature_workers import CommandError, CommandErrorCode, WorkItem
from equity_feature_workers.equity_path_gap import (
    compute_path_gap_inputs, make_ef1_path_gap_work, path_gap_headers, required_path_gap_inputs,
)
from equity_feature_workers.equity_path_drive import (
    compute_path_drive_inputs, make_ef1_path_drive_work, path_drive_headers, required_path_drive_inputs,
)
from equity_feature_workers.ef1_runner import (
    run_ef1_anchors, run_ef1_clocks, run_ef1_statuses, run_ef1_path_gaps, run_ef1_path_drives,
)
from ef1_path_fixture import ORACLE, CASES, case_record, fixture, replace_column, work
from ef1_anchor_fixture import declaration
from supplied_memory import SerializedMemorySink

LIMITS = SinkRequirements(max_results=1, max_chunk_bytes=262144, max_total_bytes=262144,
    max_result_cells=4, max_evidence_rows=7)


def calculate(item):
    return item.calculate(item.task, item.batches)[0]


def run(item, sink=None):
    runner = run_ef1_path_gaps if item.task.family == 'path_gap' else run_ef1_path_drives
    return runner((item,), sink if sink is not None else SerializedMemorySink(), requirements=LIMITS)


class EF1Paths(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import equity_features
        import equity_feature_workers
        if (equity_features.__version__, c.__version__, equity_feature_workers.__version__) != (
                '0.0.4a5', '0.0.4a4', '0.1.0a15'):
            raise AssertionError('Path suite requires exact reviewed candidate versions')

    def rejects(self, operation, code=None):
        with self.assertRaises(CommandError) as caught:
            operation()
        if code is not None:
            self.assertEqual(caught.exception.code, code)

    def nulls(self, result, status, reason, expected, observed):
        self.assertTrue(all(column.values == (None,) for column in result.values))
        self.assertTrue(all((q.status, q.expected, q.observed, q.reasons) ==
            (status, expected, observed, (reason,)) for q in result.quality))
        self.assertEqual(result.evidence, ())

    def test_fourteen_literal_values_quality_witnesses_and_full_identity(self):
        from equity_features.ef1 import compute_ef1
        for name in CASES:
            with self.subTest(case=name):
                row = case_record(name)
                item = work(name)
                result = calculate(item)
                group = ORACLE['gap' if item.task.family == 'path_gap' else 'drive']
                self.assertEqual(tuple(column.feature_id for column in result.values), tuple(group['output_order']))
                for column, expected in zip(result.values, row['expected_values_in_output_order']):
                    value = column.values[0]
                    if type(expected) is str:
                        self.assertAlmostEqual(value, float(Fraction(expected)), delta=1e-12)
                    else:
                        self.assertEqual(value, expected)
                        if expected is not None:
                            self.assertIs(type(value), type(expected))
                for quality in result.quality:
                    specification = (row['quality_each'] if 'quality_each' in row else
                        row['first_fill_quality'] if quality.feature_id.endswith('.first_fill_ns') else row['quality_other'])
                    self.assertEqual((quality.status.value, quality.expected, quality.observed,
                        [reason.value for reason in quality.reasons]), tuple(specification))
                self.assertEqual(len(result.evidence), row['evidence_count'])
                self.assertEqual(result.metadata.evidence_limit, group['evidence_limit'])
                for suffix, ids in row.get('witnesses', {}).items():
                    prefix = 'session.gap.' if item.task.family == 'path_gap' else 'session.opening_drive.'
                    self.assertEqual(sorted(e.row_id for e in result.evidence if e.feature_id == prefix + suffix), sorted(ids))
                self.assertEqual(result.metadata.inputs, tuple(sorted(
                    (slot.binding for slot in item.task.inputs if slot.binding is not None), key=lambda b: b.role)))
                self.assertEqual(result.metadata.config_digest, item.task.config.digest)
                self.assertEqual(result.metadata.backend_version, '0.0.4a5')
                self.assertEqual(encode_result(result), encode_result(compute_ef1(item.batches,
                    item.task.config, entity=c.EntityKey('X', 'S'))))
                self.assertEqual(decode_result(encode_result(result)), result)

    def test_actual_prior_and_bar_witness_identity_bounds_and_known_at(self):
        gap = calculate(work())
        prior = next(e for e in gap.evidence if e.row_id == 'daily:P')
        self.assertEqual((prior.input_id, prior.event_ns, prior.known_at_ns,
            prior.effective_start_ns, prior.effective_end_ns, prior.boundary),
            ('prior', 0, 0, -10, 0, 'completed_interval'))
        self.assertEqual(prior.entity, c.EntityKey('X', 'S'))
        bars = calculate(work('G02'))
        for row in bars.evidence:
            if row.input_id == 'path':
                self.assertEqual((row.row_id, row.event_ns, row.known_at_ns,
                    row.effective_start_ns, row.effective_end_ns, row.boundary),
                    ('bar:10:12', 12, 12, 10, 12, 'completed_interval'))
        self.assertEqual(gap.metadata.inputs[-1].metadata.scope, c.InputScope(-10, 0, 'supplied_path'))

    def test_each_omitted_role_is_real_absence_without_clones(self):
        for name in ('G01', 'D01'):
            config, slots = fixture(name)
            for index in range(len(slots)):
                absent = tuple(None if i == index else batch for i, batch in enumerate(slots))
                item = work(name, slots=absent)
                with patch('equity_features.ef1.compute_ef1', wraps=__import__(
                        'equity_features.ef1', fromlist=['compute_ef1']).compute_ef1) as pure:
                    result = calculate(item)
                self.assertEqual(pure.call_args.args[0], tuple(b for b in absent if b is not None))
                self.assertTrue(all(type(b) is c.CanonicalBatch for b in pure.call_args.args[0]))
                self.assertTrue(all(q.status is c.Status.MISSING_INPUT and c.Reason.ABSENT_INPUT in q.reasons for q in result.quality))
                self.assertEqual(len(result.metadata.inputs), len(slots) - 1)

    def test_causal_original_knowledge_for_each_role_in_both_modes(self):
        for mode in ('known_at', 'reconstruction'):
            config, slots = fixture(mode=mode)
            for index, slot in enumerate(slots):
                for known, reason in ((None, c.Reason.UNKNOWN_AVAILABILITY), (14, c.Reason.FUTURE_KNOWLEDGE)):
                    changed = list(slots)
                    changed[index] = replace_column(slot, 'known_at_ns', (known,) * slot.row_count)
                    self.nulls(calculate(work(config=config, slots=tuple(changed), mode=mode)),
                        c.Status.MISSING_INPUT, reason, 4, 4)
            equal = tuple(replace_column(slot, 'known_at_ns', (13,) * slot.row_count) for slot in slots)
            self.assertTrue(all(q.status is c.Status.AVAILABLE for q in calculate(work(config=config, slots=equal)).quality))

    def test_present_only_kind_order_role_binding_and_history_admission(self):
        item = work()
        self.rejects(lambda: compute_path_gap_inputs(item.task, tuple(reversed(item.batches))))
        self.rejects(lambda: compute_path_gap_inputs(item.task, (None,) + item.batches[1:]))
        self.rejects(lambda: compute_path_gap_inputs(replace(item.task, inputs=tuple(reversed(item.task.inputs))), item.batches))
        self.rejects(lambda: compute_path_gap_inputs(replace(item.task, family='path_drive'), item.batches))
        self.rejects(lambda: compute_path_gap_inputs(replace(item.task, features=item.task.features[:1]), item.batches), CommandErrorCode.RESULT)
        self.rejects(lambda: compute_path_gap_inputs(replace(item.task, ordered_history=True,
            initialization_sha256='0' * 64), item.batches), CommandErrorCode.RESULT)
        foreign = replace(item.batches[1], metadata=replace(item.batches[1].metadata,
            source=replace(item.batches[1].metadata.source, input_id='foreign')))
        self.rejects(lambda: compute_path_gap_inputs(item.task, (item.batches[0], foreign, item.batches[2])), CommandErrorCode.RESULT)

    def test_context_and_serialized_input_bounds_are_retained(self):
        item = work()
        self.rejects(lambda: compute_path_gap_inputs(replace(item.task, max_input_bytes=1), item.batches), CommandErrorCode.LIMIT)
        config = replace(item.task.config, parameters=(c.Parameter('ef1_context_json', 'x' * 4097),))
        self.rejects(lambda: path_gap_headers(config))
        self.rejects(lambda: compute_path_gap_inputs(replace(item.task, config=replace(
            item.task.config, identity='ef1.path_drive')), item.batches))

    def test_full_memory_readback_replay_and_distinct_config_identity(self):
        for name in ('G01', 'G02', 'D01'):
            item = work(name)
            sink = SerializedMemorySink()
            first, replay = run(item, sink), run(item, sink)
            self.assertTrue(first.readbacks[0].verified and replay.readbacks[0].verified)
            self.assertEqual(first.readbacks[0].output, replay.readbacks[0].output)
            self.assertEqual(sink.calls.count('commit'), 1)
            self.assertEqual(tuple(map(encode_result, first.readbacks[0].results)), (encode_result(calculate(item)),))
        first, second = work(), work('G02')
        for key in ('job_id', 'generation_id'):
            mixed = WorkItem(replace(second.task, **{key: getattr(first.task, key)}), second.batches, second.calculate)
            self.rejects(lambda: run_ef1_path_gaps((first, mixed), SerializedMemorySink(), requirements=LIMITS), CommandErrorCode.RESULT)

    def test_wrappers_are_closed_to_other_producers_and_callbacks(self):
        gap, drive = work(), work('D01')
        for runner, item in ((run_ef1_path_drives, gap), (run_ef1_path_gaps, drive),
                (run_ef1_anchors, gap), (run_ef1_clocks, gap), (run_ef1_statuses, gap)):
            self.rejects(lambda: runner((item,), SerializedMemorySink(), requirements=LIMITS))
        wrong = WorkItem(gap.task, gap.batches, compute_path_drive_inputs)
        self.rejects(lambda: run_ef1_path_gaps((wrong,), SerializedMemorySink(), requirements=LIMITS))

    def test_no_result_metadata_repair(self):
        from equity_features import ef1
        item = work()
        direct = calculate(item)
        for metadata in (replace(direct.metadata, inputs=()), replace(direct.metadata, config_digest='0' * 64),
                replace(direct.metadata, backend_version='0.0.4a4')):
            with patch.object(ef1, 'compute_ef1', return_value=replace(direct, metadata=metadata, evidence=())):
                self.rejects(lambda: calculate(item), CommandErrorCode.RESULT)

    def test_cancellation_and_committed_readback_failure(self):
        class Cancelled:
            def is_cancelled(self):
                return True
        item, sink = work(), SerializedMemorySink()
        cancelled = run_ef1_path_gaps((item,), sink, requirements=LIMITS, cancellation=Cancelled())
        self.assertTrue(cancelled.execution.cancelled)
        self.assertNotIn('begin', sink.calls)
        initial = run(item, sink)
        from equity_feature_workers.supervisor import BoundedSupervisor
        with patch.object(BoundedSupervisor, 'run', return_value=initial.execution), \
                patch.object(sink, 'read', side_effect=ValueError('not public')):
            failure = run(item, sink)
        self.assertEqual(failure.readbacks[0].reason, 'READBACK_FAILED')
        self.assertEqual(failure.readbacks[0].output, initial.readbacks[0].output)
        self.assertTrue(failure.readbacks[0].output.committed)

    def test_trade_endpoint_bar_end_and_future_scope_reject(self):
        config, slots = fixture('D01')
        changed = (slots[0], replace_column(slots[1], 'event_ns', (10, 12, 15)))
        self.rejects(lambda: calculate(work('D01', config=config, slots=changed)), CommandErrorCode.CONFIG)
        bar = calculate(work('G02'))
        self.assertEqual(bar.values[-1].values, (0.5,))
        config, slots = fixture()
        changed = (slots[0], replace(slots[1], metadata=replace(slots[1].metadata,
            scope=c.InputScope(10, 14, 'supplied_path'))), slots[2])
        self.rejects(lambda: calculate(work(config=config, slots=changed)))

    def test_duplicate_reverse_order_and_wrong_prior_reject(self):
        config, slots = fixture()
        for name, values in (('event_id', ('a', 'a', 'c')), ('event_ns', (12, 11, 10)),
                ('order_key', (0, 0, 2))):
            changed = replace_column(slots[1], name, values)
            if name == 'order_key':
                changed = replace_column(changed, 'event_ns', (10, 10, 12))
            self.rejects(lambda: calculate(work(config=config, slots=(slots[0], changed, slots[2]))))
        bad_prior = replace_column(slots[2], 'session_id', ('S',))
        self.rejects(lambda: calculate(work(config=config, slots=(slots[0], slots[1], bad_prior))))

    def test_canonical_invalid_cells_win_even_when_other_role_missing(self):
        config, slots = fixture()
        for name in ('price', 'size'):
            changed = replace_column(slots[1], name, (None, 11, 9) if name == 'price' else (None, 1, 1))
            self.rejects(lambda: calculate(work(config=config, slots=(None, changed, slots[2]))), CommandErrorCode.CONFIG)
        prior = replace(slots[2], columns=slots[2].columns + (c.Column('volume', (0,)),))
        self.rejects(lambda: calculate(work(config=config, slots=(slots[0], slots[1], prior))), CommandErrorCode.CONFIG)
        config, bars = fixture('G02')
        bad = replace_column(bars[1], 'close', (None,))
        self.rejects(lambda: calculate(work('G02', config=config, slots=(None, bad, bars[2]))), CommandErrorCode.CONFIG)

    def test_all_ineligible_preserves_population_without_knowledge_dependency(self):
        config, slots = fixture()
        path = replace_column(replace_column(slots[1], 'eligible', (False,) * 3), 'known_at_ns', (None,) * 3)
        self.nulls(calculate(work(config=config, slots=(slots[0], path, slots[2]))),
            c.Status.NOT_APPLICABLE, c.Reason.NO_ELIGIBLE_OBSERVATIONS, 4, 4)

    def test_quality_overflow_is_not_hidden_by_missing_proof(self):
        config, slots = fixture()
        path = replace(slots[1], metadata=replace(slots[1].metadata,
            coverage=c.Coverage((1 << 63) - 1, 3, False)))
        self.rejects(lambda: calculate(work(config=config, slots=(None, path, slots[2]))), CommandErrorCode.CALCULATION)

    def test_discovery_whole_sets_requirements_and_sampling_are_closed(self):
        for name, get_headers, get_required in (('G01', path_gap_headers, required_path_gap_inputs),
                ('G02', path_gap_headers, required_path_gap_inputs), ('D01', path_drive_headers, required_path_drive_inputs)):
            item = work(name)
            self.assertEqual(item.task.features, get_headers(item.task.config))
            self.assertTrue(get_required(item.task.config))
            self.assertEqual(item.task.max_input_batches, 3 if name.startswith('G') else 2)
        self.rejects(lambda: path_gap_headers(work('D01').task.config))
        self.rejects(lambda: path_drive_headers(work().task.config))


    def test_downward_reversal_and_equal_time_first_hit_ties(self):
        row = dict(case_record('G03'), rows=[['X','S',10,0,'a',True,10,1,10],
            ['X','S',11,1,'b',True,12,1,11], ['X','S',12,2,'c',True,11,1,12]])
        result = calculate(work('G03', record=row))
        self.assertEqual(tuple(column.values[0] for column in result.values), (1.0, True, 11, 0.5))
        row = dict(case_record('G01'), rows=[['X','S',10,0,'a',True,12,1,10],
            ['X','S',11,1,'b',True,9,1,11], ['X','S',11,2,'c',True,9,1,11]])
        result = calculate(work(record=row))
        self.assertEqual({e.row_id for e in result.evidence if e.feature_id.endswith('.filled')}, {'b'})
        self.assertEqual({e.row_id for e in result.evidence if e.feature_id.endswith('.fill_fraction')}, {'a', 'b'})

    def test_bar_high_low_only_crossings_first_positive_open_and_zero_rows(self):
        row = dict(case_record('G02'), C=13, rows=[
            ['X','S',10,11,None,None,None,None,0,None],
            ['X','S',11,12,12,14,9,11,2,12],
            ['X','S',12,13,11,15,8,11,1,13]], path_coverage=[3,3,True])
        result = calculate(work('G02', record=row))
        self.assertEqual(tuple(column.values[0] for column in result.values), (0.5, False, None, 0.5))
        self.assertTrue(all((q.expected, q.observed) == (4, 4) for q in result.quality))
        self.assertTrue(all(e.row_id != 'bar:10:11' for e in result.evidence))
        zero = dict(row, rows=[['X','S',10,13,None,None,None,None,0,None]], path_coverage=[1,1,True])
        self.nulls(calculate(work('G02', record=zero)), c.Status.NOT_APPLICABLE,
            c.Reason.NO_ELIGIBLE_OBSERVATIONS, 2, 2)

    def test_bar_indeterminate_volume_and_absent_operand_columns(self):
        row = dict(case_record('G02'), rows=[['X','S',10,12,12,13,9,11,None,12]])
        self.nulls(calculate(work('G02', record=row)), c.Status.MISSING_INPUT, c.Reason.NULL_FIELD, 2, 2)
        config, slots = fixture('G02')
        path = replace(slots[1], columns=tuple(column for column in slots[1].columns if column.name != 'close'))
        self.nulls(calculate(work('G02', config=config, slots=(slots[0], path, slots[2]))),
            c.Status.MISSING_INPUT, c.Reason.ABSENT_INPUT, 2, 2)

    def test_prior_structurally_valid_zero_null_and_absent_close(self):
        config, slots = fixture()
        prior = replace_column(slots[2], 'close', (None,))
        prior = replace(prior, columns=prior.columns + (c.Column('volume', (0,)),))
        self.nulls(calculate(work(config=config, slots=(slots[0], slots[1], prior))),
            c.Status.MISSING_INPUT, c.Reason.NULL_FIELD, 4, 4)
        prior = replace(slots[2], columns=tuple(column for column in slots[2].columns if column.name != 'close'))
        self.nulls(calculate(work(config=config, slots=(slots[0], slots[1], prior))),
            c.Status.MISSING_INPUT, c.Reason.ABSENT_INPUT, 4, 4)

    def test_drive_late_cutoff_keeps_duration_scope_and_partial_prefix_is_unavailable(self):
        config, slots = fixture('D01')
        later = replace(config, availability=c.AvailabilitySpec(40, 40, 40))
        item = work('D01', config=later, slots=slots)
        self.assertEqual(tuple(column.values[0] for column in calculate(item).values), (0.0, 0.2, 0.1))
        self.assertEqual(item.batches[1].metadata.scope, c.InputScope(10, 15, 'supplied_path'))
        self.nulls(calculate(work('D01-prefix-C14')), c.Status.INCOMPLETE_COVERAGE,
            c.Reason.GOVERNED_GAP, 2, 2)

    def test_unit_adjustment_instrument_auction_and_prior_shape_reject(self):
        config, slots = fixture()
        for metadata in (replace(slots[1].metadata, price_unit=c.PriceUnit(2, 'USD')),
                replace(slots[1].metadata, scope=c.InputScope(10, 13, 'supplied_path', True))):
            path = replace(slots[1], metadata=metadata)
            self.rejects(lambda: calculate(work(config=config, slots=(slots[0], path, slots[2]))))
        foreign = replace_column(slots[2], 'instrument_id', ('Y',))
        self.rejects(lambda: work(config=config, slots=(slots[0], slots[1], foreign)), CommandErrorCode.RESULT)
        empty_prior = replace(slots[2], columns=tuple(c.Column(column.name, ()) for column in slots[2].columns),
            metadata=replace(slots[2].metadata, coverage=c.Coverage(0, 0, True)))
        self.rejects(lambda: calculate(work(config=config, slots=(slots[0], slots[1], empty_prior))), CommandErrorCode.CONFIG)
    def test_multiple_gap_readbacks_bind_actual_task_identity(self):
        items = (work('G01'), work('G02'), work('G03'))
        sink = SerializedMemorySink()
        first = run_ef1_path_gaps(items, sink, requirements=LIMITS)
        replay = run_ef1_path_gaps(items, sink, requirements=LIMITS)
        by_task = {item.task.task_sha256: item for item in items}
        self.assertEqual(len(first.readbacks), len(items))
        self.assertEqual({row.task_sha256 for row in first.readbacks}, set(by_task))
        self.assertTrue(all(row.verified for row in first.readbacks + replay.readbacks))
        self.assertEqual({row.task_sha256: row.output for row in first.readbacks},
            {row.task_sha256: row.output for row in replay.readbacks})
        self.assertEqual(sink.calls.count('commit'), 3)
        for row in first.readbacks:
            item = by_task[row.task_sha256]
            self.assertEqual(tuple(map(encode_result, row.results)), (encode_result(calculate(item)),))
if __name__ == '__main__':
    unittest.main()
