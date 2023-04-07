import pandas as pd
import pytest
from datetime import datetime
from fastapi.testclient import TestClient
from src.server import app, filter_by_date
import os

client = TestClient(app)


@pytest.fixture
def data():
    path = os.path.join(os.path.dirname(__file__), 'data/data_test.pq.zst')
    measurements = pd.read_parquet(path, engine='pyarrow')
    measurements.reset_index(inplace=True)
    measurements.rename(columns={"index": "measured_at"}, inplace=True)
    measurements['measured_at'] = pd.to_datetime(measurements['measured_at'], unit='ms')
    return measurements


def test_health_check():
    response = client.get("/health_check")
    assert response.status_code == 200
    assert response.json() == {'msg': 'ok'}


def test_route_get_product_result_tag_no_product():
    with TestClient(app) as startup_client:
        res = startup_client.get(
            "/api/data?since=2009-10-25").json()
        assert res == {'detail': [{'loc': ['query', 'datalogger'],
                                   'msg': 'field required', 'type': 'value_error.missing'}]}


@pytest.mark.parametrize('since,before', [
    ('2021-10-25', None),
    ('2021-10-25', '2022-01-01T00:14:48.832'),
    ('2022-01-01', '2022-05-01T00:14:48.832')])
def test_filter_by_date(data, since, before):
    since = datetime.fromisoformat(since)
    before = datetime.fromisoformat(before) if before else datetime.now()
    filtered_data = filter_by_date(data, since, before)
    print(data)
    assert (since < filtered_data['measured_at']).all()
    assert (before > filtered_data['measured_at']).all()
