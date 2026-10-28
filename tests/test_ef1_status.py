"""Source-authored mandatory status qualification; execution remains pending."""
from dataclasses import replace
import json
import unittest
from unittest.mock import patch

import equity_features
import equity_feature_contracts as c
from equity_feature_io_contracts import SinkRequirements
from equity_feature_io_sdk import encode_result
from equity_feature_workers import CommandError, CommandErrorCode, InputManifest, ManifestError, ManifestErrorCode, WorkItem
from equity_feature_workers.equity_status import (
    compute_status_inputs, required_status_inputs, status_headers,
)
from equity_feature_workers.ef1_runner import run_ef1_anchors, run_ef1_clocks, run_ef1_statuses
from ef1_status_fixture import GOLDENS, ROLES, batch, fixture, make, work
from supplied_memory import SerializedMemorySink
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))  # tools/_versions.py
from _versions import CONTRACTS, FEATURES, VERSIONS  # noqa: E402


LIMITS = SinkRequirements(max_results=1, max_chunk_bytes=262144, max_total_bytes=262144,
    max_result_cells=4, max_evidence_rows=128)


def changed(batch, name, values):
    return replace(batch, columns=tuple(c.Column(column.name, values) if column.name == name
        else column for column in batch.columns))


class EF1Status(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import equity_feature_workers
        from equity_features import ef1

        if (equity_features.__version__, c.__version__, equity_feature_workers.__version__) != (FEATURES, CONTRACTS, VERSIONS['equity-feature-workers']):
            raise AssertionError('Status requires the exact reviewed candidate cohort')
        if not all(callable(getattr(ef1, name, None)) for name in (
                'build_ef1_config', 'decode_ef1_context', 'ef1_definitions', 'compute_ef1')):
            raise AssertionError('Mandatory EF1 public API absent')

    def reject(self, item, code, *, task=None, batches=None):
        with self.assertRaises(CommandError) as caught:
            compute_status_inputs(item.task if task is None else task,
                item.batches if batches is None else batches)
        self.assertEqual(caught.exception.code, code)

    def assert_parity(self, item, slots):
        from equity_features.ef1 import compute_ef1

        result, = compute_status_inputs(item.task, item.batches)
        direct = compute_ef1(slots, item.task.config, entity=c.EntityKey('X', 'S'))
        self.assertEqual(encode_result(result), encode_result(direct))
        self.assertEqual(result.metadata.inputs,
            tuple(sorted((slot.binding for slot in item.task.inputs if slot.binding is not None),
                key=lambda binding: binding.role)))
        self.assertEqual(result.metadata.config_digest, item.task.config.digest)
        return result

    def test_S01_S03_literal_values_quality_counts_and_full_SDK_parity(self):
        for case, cutoff, knowledge, maximum, state, age, admission, reason, sq, aq in GOLDENS:
            with self.subTest(case=case, cutoff=cutoff, knowledge=knowledge, maximum=maximum):
                config, slots = fixture(case, cutoff=cutoff, knowledge=knowledge, max_age=maximum)
                item = make(config, slots)
                result = self.assert_parity(item, slots)
                dimension = 'trading' if case == 'S01' else 'ssr'
                expected = {'admission_reason': reason, 'admission_state': admission,
                    dimension + '_age_ns': age, dimension + '_state': state}
                self.assertEqual({column.feature_id: column.values[0] for column in result.values},
                    {'session.status.' + name: value for name, value in expected.items()})
                for quality in result.quality:
                    name = quality.feature_id.rsplit('.', 1)[1]
                    if name.startswith('admission_'):
                        self.assertEqual((quality.status.value, quality.reasons,
                            quality.expected, quality.observed), ('available', (), 1, 1))
                    else:
                        status, reasons = sq if name.endswith('_state') else aq
                        self.assertEqual((quality.status.value, tuple(r.value for r in quality.reasons)),
                            (status, reasons))
                        count = 6 if case == 'S01' else 4 if case == 'S02' else 5
                        self.assertEqual((quality.expected, quality.observed), (count, count))

    def test_every_missing_slot_all_missing_and_covered_empty_keep_original_position(self):
        from equity_features import ef1

        for absent in ((0,), (1,), (2,), (3,), (0, 2), (1, 3), (0, 1, 2, 3)):
            config, slots = fixture(missing=absent)
            item = make(config, slots)
            original = ef1.compute_ef1
            with patch.object(ef1, 'compute_ef1', wraps=original) as called:
                result = self.assert_parity(item, slots)
            passed = called.call_args_list[0].args[0]
            self.assertEqual(len(passed), 4)
            self.assertTrue(all(actual is expected for actual, expected in zip(passed, slots)))
            factual = [q for q in result.quality if not q.feature_id.rsplit('.', 1)[1].startswith('admission_')]
            self.assertTrue(all(q.status is c.Status.MISSING_INPUT and c.Reason.ABSENT_INPUT in q.reasons
                for q in factual))
            self.assertEqual(result.evidence, ())
        for empty in ((0,), (1,), (2,), (3,)):
            config, slots = fixture(empty=empty, missing=(2,) if empty == (1,) else ())
            item = make(config, slots)
            self.assertEqual(item.task.inputs[empty[0]].state, 'empty')
            result = self.assert_parity(item, slots)
            self.assertIn(c.InputBinding(ROLES[empty[0]], slots[empty[0]].kind,
                slots[empty[0]].metadata), result.metadata.inputs)
            self.assertTrue(any(q.status is c.Status.MISSING_INPUT for q in result.quality))

    def test_actual_manifest_roles_order_and_batch_bijection_reject(self):
        item = work()
        for inputs, supplied in ((item.task.inputs[:-1], item.batches[:-1]),
                (item.task.inputs[::-1], item.batches[::-1]),
                (item.task.inputs + (replace(item.task.inputs[-1], role='extra',
                    state='missing', binding=None),), item.batches)):
            self.reject(item, CommandErrorCode.CONFIG, task=replace(item.task, inputs=inputs),
                batches=supplied)
        self.reject(item, CommandErrorCode.RESULT, batches=item.batches[:-1])
        self.reject(item, CommandErrorCode.LIMIT, batches=item.batches + (item.batches[0],))
        self.reject(item, CommandErrorCode.RESULT, batches=item.batches[::-1])
        self.reject(item, CommandErrorCode.CONFIG, task=replace(item.task, family='clock'))
        self.reject(item, CommandErrorCode.RESULT, task=replace(item.task, features=item.task.features[:2]))
        foreign = replace(item.batches[1], metadata=replace(item.batches[1].metadata,
            source=replace(item.batches[1].metadata.source, input_id='foreign')))
        self.reject(item, CommandErrorCode.RESULT,
            batches=(item.batches[0], foreign, *item.batches[2:]))

    def test_populated_contradictory_roles_reject_in_pure_calculation(self):
        config, slots = fixture()
        # Independently assign the exchanged populated declarations too; generic
        # metadata admission passes, while the pure role vocabulary contradicts.
        swapped = (slots[0], slots[1], slots[3], slots[2])
        self.reject(make(config, swapped), CommandErrorCode.CONFIG)

    def test_context_and_actual_task_bounds_remain_independent(self):
        item = work()
        self.reject(item, CommandErrorCode.LIMIT, task=replace(item.task, max_input_bytes=1))
        self.reject(item, CommandErrorCode.LIMIT, task=replace(item.task, max_input_batches=3))
        self.reject(item, CommandErrorCode.LIMIT, task=replace(item.task, max_input_bytes=1048577))
        with self.assertRaises((CommandError, c.ContractError, ManifestError)):
            replace(item.task, config=replace(item.task.config,
                parameters=(c.Parameter('ef1_context_json', 'x' * 4097),)))
        oversized = tuple(InputManifest('r' + str(index) + 'x' * 4000, '0' * 64,
            'r' * 4000, 'missing', None) for index in range(140))
        with self.assertRaises(ManifestError) as caught:
            replace(item.task, inputs=oversized)
        self.assertEqual(caught.exception.code, ManifestErrorCode.LIMIT)
        self.reject(item, CommandErrorCode.RESULT, task=replace(item.task,
            ordered_history=True, initialization_sha256='0' * 64))

    def test_original_selected_witness_and_stale_age_are_preserved(self):
        cases = (
            ('S01', dict(cutoff=16), 's1:t:ZQ:0000000000000000001:aGFsdA', 12, 12, False),
            ('S01', dict(cutoff=17), 's1:t:ZQ:0000000000000000002:cmVzdW1l', 16, 16, False),
            ('S02', dict(max_age=10), 's1:s:ZQ:0000000000000000000:c2VlZA', 5, 5, True),
            ('S03', {}, 's1:s:ZQ:0000000000000000001:Yg', 5, 5, False),
        )
        for case, options, row_id, event, known, stale in cases:
            item = work(case, **options)
            result, = compute_status_inputs(item.task, item.batches)
            self.assertEqual(len(result.evidence), 2)
            for evidence in result.evidence:
                self.assertEqual((evidence.input_id, evidence.row_id, evidence.event_ns, evidence.known_at_ns,
                    evidence.effective_start_ns, evidence.effective_end_ns), ('st', row_id, event, known, event, None))
                excluded = stale and evidence.feature_id.endswith('_state')
                self.assertEqual((evidence.use, evidence.exclusion_reason),
                    ('excluded', c.Reason.INELIGIBLE) if excluded else ('consumed', None))
        for options in (dict(knowledge=15), dict(missing=(2,)), dict(seed=False)):
            item = work(**options)
            result, = compute_status_inputs(item.task, item.batches)
            self.assertEqual(result.evidence, ())
            self.assertTrue(all(column.values == (None,) for column in result.values
                if column.feature_id.endswith(('_state', '_age_ns'))
                and not column.feature_id.endswith('admission_state')))

    def test_required_transition_and_calendar_knowledge_remain_causal_in_both_modes(self):
        for mode in ('known_at', 'reconstruction'):
            config, slots = fixture(knowledge=16, mode=mode)
            equality = self.assert_parity(make(config, slots), slots)
            self.assertTrue(all(q.status is c.Status.AVAILABLE for q in equality.quality))
            config, slots = fixture(knowledge=15, mode=mode)
            result = self.assert_parity(make(config, slots), slots)
            self.assertEqual(result.evidence, ())
            for index in (0, 1, 2, 3):
                config, slots = fixture(mode=mode)
                for known, reason in ((None, c.Reason.UNKNOWN_AVAILABILITY), (18, c.Reason.FUTURE_KNOWLEDGE)):
                    original = slots[index]
                    mutation = changed(original, 'known_at_ns', tuple(known for _ in range(original.row_count)))
                    supplied = (*slots[:index], mutation, *slots[index + 1:])
                    result = self.assert_parity(make(config, supplied), supplied)
                    self.assertEqual(result.evidence, ())
                    factual = [q for q in result.quality if q.feature_id.endswith(('_state', '_age_ns'))
                        and not q.feature_id.endswith('admission_state')]
                    self.assertTrue(all(q.status is c.Status.MISSING_INPUT and reason in q.reasons for q in factual))

    def test_complete_prefix_then_expired_validity_literal_separate_mutation(self):
        item = work('S02', cutoff=17, prefix_end=20, validity_end=17)
        result, = compute_status_inputs(item.task, item.batches)
        self.assertEqual({column.feature_id.rsplit('.', 1)[1]: column.values[0] for column in result.values},
            dict(admission_reason='outside_effective_interval', admission_state='unavailable', ssr_age_ns=None, ssr_state=None))
        self.assertEqual(result.evidence, ())
        self.assertTrue(all((q.status, q.reasons) == (c.Status.MISSING_INPUT, (c.Reason.INELIGIBLE,))
            for q in result.quality if q.feature_id.endswith(('ssr_state', 'ssr_age_ns'))))

    def test_finite_original_state_bounds_are_witnessed_then_expire_at_equality(self):
        config, slots = fixture()
        for end, available in ((19, True), (17, False)):
            states = changed(slots[1], 'effective_end_ns', (None, None, end))
            supplied = (slots[0], states, slots[2], slots[3])
            result = self.assert_parity(make(config, supplied), supplied)
            if available:
                self.assertEqual(len(result.evidence), 2)
                self.assertTrue(all((e.row_id, e.event_ns, e.known_at_ns, e.effective_start_ns,
                    e.effective_end_ns, e.use) == ('s1:t:ZQ:0000000000000000002:cmVzdW1l',
                    16, 16, 16, 19, 'consumed') for e in result.evidence))
            else:
                self.assertEqual(result.evidence, ())
                self.assertTrue(all(q.status is c.Status.MISSING_INPUT and q.reasons == (c.Reason.INELIGIBLE,)
                    for q in result.quality if q.feature_id.endswith(('trading_state', 'trading_age_ns'))))

    def test_proof_identity_and_noncanonical_reference_encoding_reject(self):
        config, slots = fixture()
        for index, name, values, code in (
                (2, 'reference_id', ('p1:t:ZQ:d3Jvbmc:cHJlZg',), CommandErrorCode.RESULT),
                (3, 'reference_id', ('p1:t:ZQ=:c3Q:dmFsaWQ',), CommandErrorCode.CONFIG),
                (0, 'reference_id', ('c1:' + '0' * 64,), CommandErrorCode.RESULT),
                (1, 'reference_id', ('s1:t:ZQ:0000000000000000000:c2VlZA',
                    's1:t:ZQ:0000000000000000000:aGFsdA',
                    's1:t:ZQ:0000000000000000002:cmVzdW1l'), CommandErrorCode.CONFIG)):
            supplied = (*slots[:index], changed(slots[index], name, values), *slots[index + 1:])
            self.reject(make(config, supplied), code)

    def test_exact_four_headers_discovery_and_whole_result_identity(self):
        from equity_features import ef1

        item = work()
        self.assertEqual(len(status_headers(item.task.config)), 4)
        expected = []
        for definition in ef1.ef1_definitions(item.task.config):
            for requirement in definition.requirements:
                if requirement not in expected:
                    expected.append(requirement)
        self.assertEqual(required_status_inputs(item.task.config), tuple(expected))
        config, slots = fixture()
        direct = ef1.compute_ef1(slots, config, entity=c.EntityKey('X', 'S'))
        for metadata in (replace(direct.metadata, inputs=direct.metadata.inputs[1:]),
                replace(direct.metadata, config_digest='0' * 64),
                replace(direct.metadata, backend_version='0.0.4a4')):
            with patch.object(ef1, 'compute_ef1', return_value=replace(direct, metadata=metadata)):
                self.reject(item, CommandErrorCode.RESULT)

    def test_quoting_is_one_homogeneous_four_output_selection(self):
        from equity_features.ef1 import build_ef1_config

        original, slots = fixture('S02')
        context = json.loads(original.parameters[0].value)
        context['selection'] = dict(dimension='quoting', epoch='e', max_age_ns=None,
            seed_reference_id='s1:q:ZQ:0000000000000000000:c2VlZA')
        config = build_ef1_config(context, session=original.session, window=original.window,
            availability=original.availability, price_unit=None, adjustment=original.adjustment)
        supplied = (slots[0],
            batch('st', (('s1:q:ZQ:0000000000000000000:c2VlZA', 'ef1.status.quoting', 5, None, 'disabled', 5),)),
            batch('prefix', (('p1:q:ZQ:c3Q:cHJlZg', 'ef1.status.prefix_complete', 5, 16, 'q', 5),)),
            batch('validity', (('p1:q:ZQ:c3Q:dmFsaWQ', 'ef1.status.validity', 5, 17, 'q', 5),)))
        result = self.assert_parity(make(config, supplied, name='quoting'), supplied)
        self.assertEqual({column.feature_id: column.values[0] for column in result.values}, {
            'session.status.admission_reason': 'none', 'session.status.admission_state': 'ready',
            'session.status.quoting_age_ns': 11, 'session.status.quoting_state': 'disabled'})
        self.assertTrue(all(q.status is c.Status.AVAILABLE for q in result.quality))
        self.assertNotEqual(config.digest, original.digest)

    def test_certificates_do_not_arbitrate_or_stitch_and_absent_states_keep_relations(self):
        config, slots = fixture('S02')
        overlapping = batch('prefix', (
            ('p1:s:ZQ:c3Q:YQ', 'ef1.status.prefix_complete', 5, 20, 's', 5),
            ('p1:s:ZQ:c3Q:Yg', 'ef1.status.prefix_complete', 5, 20, 's', 18)))
        self.reject(make(config, (slots[0], slots[1], overlapping, slots[3])), CommandErrorCode.CONFIG)
        fragments = batch('prefix', (
            ('p1:s:ZQ:c3Q:YQ', 'ef1.status.prefix_complete', 5, 10, 's', 5),
            ('p1:s:ZQ:c3Q:Yg', 'ef1.status.prefix_complete', 10, 20, 's', 5)))
        item = make(config, (slots[0], slots[1], fragments, slots[3]))
        result, = compute_status_inputs(item.task, item.batches)
        self.assertEqual(result.evidence, ())
        self.assertTrue(all(c.Reason.GOVERNED_GAP in q.reasons for q in result.quality
            if q.feature_id.endswith(('ssr_state', 'ssr_age_ns'))))
        disjoint = batch('prefix', (
            ('p1:s:ZQ:c3Q:YQ', 'ef1.status.prefix_complete', 0, 4, 's', 5),
            ('p1:s:ZQ:c3Q:Yg', 'ef1.status.prefix_complete', 5, 16, 's', 5)))
        supplied = (slots[0], slots[1], disjoint, slots[3])
        result = self.assert_parity(make(config, supplied), supplied)
        self.assertTrue(all(q.status is c.Status.AVAILABLE for q in result.quality))
        wrong = changed(slots[3], 'reference_id', ('p1:s:ZQ:b3RoZXI:dmFsaWQ',))
        self.reject(make(config, (slots[0], None, slots[2], wrong)), CommandErrorCode.RESULT)

    def test_aggregate_reference_bound_is_not_a_per_slot_allowance(self):
        config, slots = fixture('S02')
        for count, accepted in ((4093, True), (4094, False)):
            rows = tuple(('s1:s:ZQ:' + format(index, '019d') + ':' + ('c2VlZA' if index == 0 else 'YQ'),
                'ef1.status.ssr', 5, None, 'on', 5) for index in range(count))
            supplied = (slots[0], batch('st', rows), slots[2], slots[3])
            item = make(config, supplied)
            if accepted:
                result = self.assert_parity(item, supplied)
                self.assertTrue(all(q.status is c.Status.AVAILABLE for q in result.quality))
                self.assertEqual(len(result.evidence), 2)
            else:
                self.reject(item, CommandErrorCode.CONFIG)

    def test_unknown_population_count_stays_unknown_and_no_fact_fallback(self):
        config, slots = fixture()
        states = replace(slots[1], metadata=replace(slots[1].metadata,
            coverage=c.Coverage(None, 3, False)))
        supplied = (slots[0], states, slots[2], slots[3])
        item = make(config, supplied)
        self.assertEqual(item.task.inputs[1].state, 'partial')
        result = self.assert_parity(item, supplied)
        for quality in result.quality:
            if quality.feature_id.endswith(('trading_state', 'trading_age_ns')):
                self.assertEqual((quality.expected, quality.observed, quality.status, quality.reasons),
                    (None, 6, c.Status.INCOMPLETE_COVERAGE, (c.Reason.GOVERNED_GAP,)))
        self.assertEqual(result.evidence, ())

    def test_result_object_and_actual_batches_are_never_relabelled_or_copied(self):
        from equity_features import ef1

        config, slots = fixture()
        item = make(config, slots)
        direct = ef1.compute_ef1(slots, config, entity=c.EntityKey('X', 'S'))
        with patch.object(ef1, 'compute_ef1', return_value=direct) as called:
            result, = compute_status_inputs(item.task, item.batches)
        self.assertIs(result, direct)
        self.assertTrue(all(actual is original for actual, original in zip(called.call_args.args[0], slots)))

    def test_full_readback_replay_and_configuration_scopes(self):
        items = (work(name='trading'), work('S02', name='ssr'), work('S03', name='tie'))
        sink = SerializedMemorySink()
        first = run_ef1_statuses(items, sink, requirements=LIMITS)
        replay = run_ef1_statuses(items, sink, requirements=LIMITS)
        self.assertTrue(all(row.verified for row in first.readbacks + replay.readbacks))
        self.assertEqual([row.output for row in first.readbacks], [row.output for row in replay.readbacks])
        self.assertEqual(sink.calls.count('commit'), 3)
        by_task = {item.task.task_sha256: item for item in items}
        self.assertEqual(len(first.readbacks), len(by_task))
        self.assertEqual({row.task_sha256 for row in first.readbacks}, set(by_task))
        for row in first.readbacks:
            item = by_task[row.task_sha256]
            self.assertEqual(tuple(map(encode_result, row.results)),
                tuple(map(encode_result, compute_status_inputs(item.task, item.batches))))
        for field in ('job_id', 'generation_id'):
            merged = WorkItem(replace(items[1].task, **{field: getattr(items[0].task, field)}),
                items[1].batches, items[1].calculate)
            with self.assertRaises(CommandError):
                run_ef1_statuses((items[0], merged), SerializedMemorySink(), requirements=LIMITS)

    def test_closed_wrappers_and_postcommit_failure_preserve_factual_receipt(self):
        from equity_feature_workers.supervisor import BoundedSupervisor
        from equity_feature_workers.equity_clock import compute_clock_inputs

        item = work()
        for runner in (run_ef1_anchors, run_ef1_clocks):
            with self.assertRaises(CommandError):
                runner((item,), SerializedMemorySink(), requirements=LIMITS)
        with self.assertRaises(CommandError):
            run_ef1_statuses((WorkItem(item.task, item.batches, compute_clock_inputs),),
                SerializedMemorySink(), requirements=LIMITS)
        sink = SerializedMemorySink()
        initial = run_ef1_statuses((item,), sink, requirements=LIMITS)
        with patch.object(BoundedSupervisor, 'run', return_value=initial.execution), \
             patch.object(sink, 'read', side_effect=ValueError('private text')):
            failed = run_ef1_statuses((item,), sink, requirements=LIMITS)
        self.assertEqual(failed.readbacks[0].reason, 'READBACK_FAILED')
        self.assertEqual(failed.readbacks[0].output, initial.readbacks[0].output)
        self.assertTrue(failed.readbacks[0].output.committed)

    def test_cancelled_status_run_never_begins_publication(self):
        class Cancelled:
            def is_cancelled(self):
                return True

        sink = SerializedMemorySink()
        outcome = run_ef1_statuses((work(),), sink, requirements=LIMITS, cancellation=Cancelled())
        self.assertTrue(outcome.execution.cancelled)
        self.assertNotIn('begin', sink.calls)


if __name__ == '__main__':
    unittest.main()
