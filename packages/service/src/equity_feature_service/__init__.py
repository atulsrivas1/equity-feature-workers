"""Optional service; native dependencies load only after bootstrap containment."""
from importlib import import_module
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .codec import WireError
    from .datasets import DatasetIdentity, FeatureDataset, RawDataset, RawRead, Scope
    from .service import Credential, Grant, Ledger, Limits, Service

__version__ = "0.1.0a3"
__all__ = ["WireError", "DatasetIdentity", "FeatureDataset", "RawDataset", "RawRead", "Scope",
           "Credential", "Grant", "Ledger", "Limits", "Service"]

_EXPORTS = {name: ".codec" if name == "WireError" else ".datasets" if name in (
    "DatasetIdentity", "FeatureDataset", "RawDataset", "RawRead", "Scope") else ".service"
    for name in __all__}


def __getattr__(name: str) -> Any:
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(name)
    value = getattr(import_module(module, __name__), name)
    globals()[name] = value
    return value
