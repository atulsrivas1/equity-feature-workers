"""Shared admission for explicitly supplied local calculations; no acquisition."""
from __future__ import annotations

from contextlib import contextmanager
import pickle
import sys
from typing import Iterator, NoReturn

from equity_feature_contracts import CanonicalBatch, ContractError, ErrorCode, FeatureResult
from equity_feature_io_contracts import SinkError, SinkErrorCode
from equity_feature_io_sdk import descriptor

from .commands import CommandError, CommandErrorCode, contract_code_of
from .manifests import ManifestError, ManifestErrorCode, TaskManifest


def fail(code: CommandErrorCode = CommandErrorCode.CONFIG, contract_code: str | None = None) -> NoReturn:
    raise CommandError(code, contract_code or contract_code_of(sys.exc_info()[1])) from None


@contextmanager
def translated() -> Iterator[None]:
    """Translate classes explicitly; unavailable inputs remain ordinary results."""
    try:
        yield
    except CommandError as error:
        if type(error.code) is not CommandErrorCode:
            fail(CommandErrorCode.CALCULATION)
        raise CommandError(error.code, error.contract_code) from None
    except ContractError as error:
        if type(error.code) is not ErrorCode:
            fail(CommandErrorCode.CALCULATION)
        fail(CommandErrorCode.RESULT if error.code is ErrorCode.INCONSISTENT_IDENTITY else
             CommandErrorCode.CALCULATION if error.code is ErrorCode.OVERFLOW else
             CommandErrorCode.CONFIG)
    except ManifestError as error:
        if type(error.code) is not ManifestErrorCode:
            fail(CommandErrorCode.CALCULATION)
        fail(CommandErrorCode.LIMIT if error.code is ManifestErrorCode.LIMIT else
             CommandErrorCode.RESULT if error.code is ManifestErrorCode.BINDING else
             CommandErrorCode.CONFIG)
    except SinkError as error:
        if type(error.code) is not SinkErrorCode:
            fail(CommandErrorCode.CALCULATION)
        fail(CommandErrorCode.LIMIT if error.code is SinkErrorCode.RESOURCE_LIMIT else CommandErrorCode.CONFIG)
    except Exception:
        fail(CommandErrorCode.CALCULATION)


def admit(task: TaskManifest, batches: tuple[CanonicalBatch, ...]) -> None:
    if type(task) is not TaskManifest or type(batches) is not tuple or any(type(b) is not CanonicalBatch for b in batches):
        fail()
    if len(task.instruments) != 1:
        fail()
    if len(batches) > task.max_input_batches:
        fail(CommandErrorCode.LIMIT)
    if len(pickle.dumps(batches, protocol=5)) > task.max_input_bytes:
        fail(CommandErrorCode.LIMIT)
    actual = tuple(i.binding for i in task.inputs if i.binding is not None)
    if len(actual) != len(batches) or len({b.metadata.source.input_id for b in batches}) != len(batches):
        fail(CommandErrorCode.RESULT)
    for binding, batch in zip(actual, batches):
        if (binding.kind, binding.metadata) != (batch.kind, batch.metadata) or batch.metadata.coverage.observed != batch.row_count:
            fail(CommandErrorCode.RESULT)
        if batch.metadata.namespace != task.config.session.namespace:
            fail(CommandErrorCode.RESULT)
        instruments = batch.column('instrument_id')
        if instruments is None:
            fail()
        if any(value != task.instruments[0] for value in instruments.values):
            fail(CommandErrorCode.RESULT)


def result_identity(task: TaskManifest, result: FeatureResult, backend: str, version: str) -> None:
    metadata = result.metadata
    expected = tuple(sorted((i.binding for i in task.inputs if i.binding is not None), key=lambda b: b.role))
    if (metadata.namespace, metadata.session_id, metadata.config_digest, metadata.availability,
        metadata.inputs, metadata.backend_id, metadata.backend_version, descriptor(result).features) != (
            task.config.session.namespace, task.config.session.session_id, task.config.digest,
            task.config.availability, expected, backend, version, task.features):
        fail(CommandErrorCode.RESULT)
