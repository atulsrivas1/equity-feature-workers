"""Optional bounded service; never imported by calculation/worker packages."""
from .codec import WireError
from .datasets import DatasetIdentity, FeatureDataset, RawDataset, RawRead, Scope
from .service import Credential, Grant, Ledger, Limits, Service

__version__ = "0.1.0a2"
__all__ = ["WireError", "DatasetIdentity", "FeatureDataset", "RawDataset", "RawRead", "Scope",
           "Credential", "Grant", "Ledger", "Limits", "Service"]
