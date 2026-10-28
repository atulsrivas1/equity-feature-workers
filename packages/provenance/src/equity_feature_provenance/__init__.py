"""Explicit outer provenance binding; pure calculation roots never import this."""
from ._model import (AcquiredInput, TaskEvidence, InputLink, ProvenanceReport,
    GenerationProvenanceReport, ProvenanceError)
from ._bind import bind_supplied_provenance, verify_live_generation

__version__ = '0.1.0a3'
__all__ = ['AcquiredInput','TaskEvidence','InputLink','ProvenanceReport',
    'GenerationProvenanceReport','ProvenanceError','bind_supplied_provenance',
    'verify_live_generation','__version__']
