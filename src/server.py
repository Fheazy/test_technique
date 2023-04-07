import requests
import sqlite3
import pandas as pd
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta
from pydantic import BaseModel
from typing import List
from fastapi import FastAPI
import logging

app = FastAPI()
G_LOGGER_UVICORN = logging.getLogger('uvicorn')


@app.get('/health_check')
async def health_check():
    return {'msg': 'ok'}


class DataRecordResponse(BaseModel):
    label: str
    measured_at: str   # "date-time"
    value: float


class DataRecordAggregateResponse(BaseModel):
    label: str
    time_slot: str  # "date-time"
    value: float


def get_json_data(datalogger):
    conn = sqlite3.connect("data/measurements.db")
    data_url = "http://localhost:3000/db"
    response = requests.get(data_url)
    if response.status_code == 200:
        data = response.json()
        data = pd.DataFrame(data['measurements'][int(datalogger)])
        measurements = data.transpose()
        measurements.reset_index(inplace=True)
        measurements.rename(columns={"index": "measured_at"}, inplace=True)
        measurements['measured_at'] = pd.to_datetime(measurements['measured_at'], unit='ms')
        measurements.to_sql('measurements', conn, if_exists='replace')
        return measurements


def filter_by_date(data, since, before):
    data = data[data["measured_at"] <= before]
    if since:
        data = data[data["measured_at"] >= since]
    return data


@app.get("/api/data", response_model=List[DataRecordResponse])
async def api_fetch_data_raw(datalogger, since=None, before=None):
    if not datalogger:
        return JSONResponse(content={"error": "Missing required values."}, status_code=400)
    records = []
    data = get_json_data(datalogger)
    since = datetime.fromisoformat(since)
    before = datetime.fromisoformat(before) if before else datetime.now()
    data = filter_by_date(data, since, before)
    data = data.fillna('')
    for label in ["temp", "precip", "hum"]:
        for index, row in data.iterrows():
            if row[label]:
                records.append(DataRecordResponse(label=label, measured_at=row['measured_at'].isoformat(),
                               value=float(row[label])))
    return records


@app.get("/api/summary", response_model=List[DataRecordAggregateResponse])
async def api_fetch_data_aggregates(since="raw", before=None, span=None, datalogger=None):
    if not datalogger:
        return JSONResponse(content={"error": "Missing required values."}, status_code=400)
    records = []
    data = get_json_data(datalogger)
    since = datetime.fromisoformat(since)
    before = datetime.fromisoformat(before) if before else datetime.now()
    data = filter_by_date(data, since, before)
    data = data.rename(columns={"measured_at": "time_slot"})
    # Aggregate by span
    if span == "raw":
        data = data.fillna('')
        for label in ["temp", "precip", "hum"]:
            for index, row in data.iterrows():
                if row[label]:
                    records.append(DataRecordAggregateResponse(label=label, time_slot=row['time_slot'].isoformat(),
                                                               value=float(row[label])))
    elif span == 'hour':
        data["time_slot"] = data["time_slot"].dt.floor('H')
        data_mean = data.groupby([pd.Grouper(key='time_slot', freq='H')])[["hum", "temp"]].mean()
        data_mean.reset_index(inplace=True)
        data_sum = data.groupby([pd.Grouper(key='time_slot', freq='H')])[["precip"]].sum()
        data_sum.reset_index(inplace=True)
        for label in ["temp", "hum"]:
            data_mean = data_mean.fillna('')
            for index, row in data_mean.iterrows():
                if row[label]:
                    records.append(DataRecordAggregateResponse(
                        label=label, time_slot=row['time_slot'].isoformat() + ', ' + str(row['time_slot'] +
                                                                                         timedelta(hours=1)),
                        value=float(row[label])))
        label = 'precip'
        data_sum = data_sum.fillna('')
        for index, row in data_sum.iterrows():
            if row[label]:
                records.append(DataRecordAggregateResponse(label=label, time_slot=row['time_slot'].isoformat(),
                                                           value=float(row[label])))
    elif span == 'day':
        data["time_slot"] = data["time_slot"].dt.floor('D')
        data_mean = data.groupby([pd.Grouper(key='time_slot', freq='D')])[["hum", "temp"]].mean()
        data_mean.reset_index(inplace=True)
        data_sum = data.groupby([pd.Grouper(key='time_slot', freq='D')])[["precip"]].sum()
        data_sum.reset_index(inplace=True)
        for label in ["temp", "hum"]:
            data_mean = data_mean.fillna('')
            for index, row in data_mean.iterrows():
                if row[label]:
                    records.append(DataRecordAggregateResponse(
                        label=label, time_slot=row['time_slot'].isoformat() + ', ' + str(row['time_slot'] +
                                                                                         timedelta(days=1)),
                        value=float(row[label])))

        label = 'precip'
        data_sum = data_sum.fillna('')
        for index, row in data_sum.iterrows():
            if row[label]:
                records.append(DataRecordAggregateResponse(label=label, time_slot=row['time_slot'].isoformat(),
                                                           value=float(row[label])))

    elif span == 'max':
        data["time_slot"] = str(data["time_slot"].min()) + ', ' + str(data['time_slot'].max())
        data_max = data[["hum", "temp"]].max()
        data_sum = data[["precip"]].sum()
        for label in ["temp", "hum"]:
            data_mean = data_max.fillna('')
            if data_mean[label]:
                records.append(DataRecordAggregateResponse(label=label, time_slot=data['time_slot'].iloc[0],
                                                           value=float(data_max[label])))
        label = 'precip'
        data_sum = data_sum.fillna('')
        if data_sum[label]:
            records.append(DataRecordAggregateResponse(label=label, time_slot=data['time_slot'].iloc[0],
                                                       value=float(data_sum[label])))

    return records


if __name__ == '__main__':
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=4151, log_level="debug", lifespan="on")
