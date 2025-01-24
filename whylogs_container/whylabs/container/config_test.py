from whylogs_container_types import DatasetCadence, DatasetUploadCadenceGranularity


# TODO move this
def test_indexing_into_toolkit() -> None:
    # Should not have errors
    daily = DatasetCadence("DAILY")
    assert daily == DatasetCadence.DAILY

    daily_granularity = DatasetUploadCadenceGranularity("D")
    assert daily_granularity == DatasetUploadCadenceGranularity.DAY
