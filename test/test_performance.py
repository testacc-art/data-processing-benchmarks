from data_processing_benchmarks.pandas import do_pandas_test
from data_processing_benchmarks.polars import do_polars_test


def test_pandas():
    res = do_pandas_test
    assert res is not None


def test_polars():
    res = do_polars_test
    assert res is not None
