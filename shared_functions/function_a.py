from datetime import datetime, timedelta
from time import time
import faust
import logging
import random

try:
    sys.path.append('../data_processing/processing_module')
except:
    pass

from app import app
logger = logging.getLogger(__name__)

TOPIC = 'raw-event'
SINK = 'agg-event-a'
TABLE = 'table_a'
CLEANUP_INTERVAL = 7.5
WINDOW = 5
WINDOW_EXPIRES = 10
PARTITIONS = 1


class RawModel(faust.Record):
    date: datetime
    value: float


class AggModelA(faust.Record):
    date: datetime
    count: int
    mean: float


app.conf.table_cleanup_interval = CLEANUP_INTERVAL
source = app.topic(TOPIC, partitions=PARTITIONS, value_type=RawModel)
sink = app.topic(SINK, value_type=AggModelA)


def window_processor(key, events):
    timestamp = key[1][0]
    values = [event.value for event in events]
    count = len(values)
    mean = sum(values) / count

    logger.info(f"""
    processing window:
    {len(values)} events,
    mean: {mean:.2f},
    timestamp {timestamp}
    """)

    sink.send_soon(value=AggModelA(date=timestamp, count=count, mean=mean))


tumbling_table = (app.Table(
    TABLE,
    default=list,
    partitions=PARTITIONS,
    on_window_close=window_processor,
).tumbling(WINDOW,
           expires=timedelta(seconds=WINDOW_EXPIRES)).relative_to_field(
               RawModel.date))


@app.agent(source)
async def print_windowed_events(stream):
    async for event in stream:
        value_list = tumbling_table['events'].value()
        value_list.append(event)
        tumbling_table['events'] = value_list


@app.timer(0.1)
async def produce():
    await source.send(value=RawModel(value=random.random(), date=int(time())))
