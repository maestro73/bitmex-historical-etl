import datetime

from bitmex_historical_etl import S3Downloader


def test_s3_downloader_200():
    now = datetime.datetime.utcnow()
    delta = now - datetime.timedelta(days=2)
    two_days_ago = delta.date()
    data_frame = S3Downloader().download(two_days_ago)
    assert len(data_frame)


def test_s3_downloader_404():
    now = datetime.datetime.utcnow()
    delta = now + datetime.timedelta(days=1)
    tomorrow = delta.date()
    data_frame = S3Downloader().download(tomorrow)
    assert data_frame is None
