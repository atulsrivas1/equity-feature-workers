from equity_feature_io_contracts import FeatureHeader
from equity_feature_workers.intraday import intraday_headers

headers: tuple[FeatureHeader, ...] = intraday_headers(('intraday.band_middle', 'intraday.ema'))
assert tuple(h.feature_id for h in headers) == ('intraday.band_middle', 'intraday.ema')
