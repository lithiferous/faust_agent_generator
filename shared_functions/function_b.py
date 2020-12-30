from datetime import datetime, timedelta
from time import time
import faust
import logging
import os
import random
import statistics as stats
import sys

if os.environ.get('DEV') is None:
    sys.path.append('../data_processing/processing_module')

from processing_module.app import app
logger = logging.getLogger(__name__)

TOPIC = 'raw-event'
SINK = 'agg-event-b'
TABLE = 'table_b'
CLEANUP_INTERVAL = 7.5
WINDOW = 3
WINDOW_EXPIRES = 10
PARTITIONS = 1


class RawModel(faust.Record):
    date: datetime
    value: float


class AggModelB(faust.Record):
    date: datetime
    count: int
    std: float


app.conf.table_cleanup_interval = CLEANUP_INTERVAL
source = app.topic(TOPIC, partitions=PARTITIONS, value_type=RawModel)
sink = app.topic(SINK, value_type=AggModelB)


def window_processor(key, events):
    timestamp = key[1][0]
    values = [event.value for event in events]
    count = len(values)
    std = stats.stdev(values)

    logger.info(f"""
    processing window:
    {len(values)} events,
    std: {std:.2f},
    timestamp {timestamp}
    """)

    sink.send_soon(value=AggModelB(date=timestamp, count=count, std=std))


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
