"""Experimental explicit remote client; native conversion is optional."""
from .models import DatasetKey, Failure, FeatureRef, JobExpectation, Outcome, ProducerExpectation, RawExpectation, Request, ScopeKey
from .client import RemoteClient
from ._response import DiscoveryView, FeatureSliceView, JobView, RawSliceView, ResultView
from importlib.util import find_spec


def native_available() -> bool:
    """Report installed optional public contracts/SDK without importing them."""
    return all(find_spec(name) is not None for name in ('equity_feature_contracts', 'equity_feature_io_contracts', 'equity_feature_io_sdk'))

__all__ = ['DatasetKey', 'Failure', 'FeatureRef', 'JobExpectation', 'Outcome', 'ProducerExpectation', 'RawExpectation', 'Request', 'ScopeKey', 'RemoteClient', 'DiscoveryView', 'FeatureSliceView', 'JobView', 'RawSliceView', 'ResultView', 'native_available']
