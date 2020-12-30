from datetime import datetime, timedelta
from time import time
import faust
import logging
import os
import pytest
import random
import sys

if os.environ.get('DEV') is None:
    sys.path.append('../data_processing')

from processing_module.app import app
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
        yield event


@app.timer(0.1)
async def produce():
    await source.send(value=RawModel(value=random.random(), date=int(time())))


@pytest.fixture()
def test_app(event_loop):
    app.finalize()
    app.flow_control.resume()
    return app


@pytest.mark.asyncio()
async def test_count_page_views(test_app):
    app.conf.store = 'memory://'
    async with print_windowed_events.test_context() as agent:
        val, dt = random.random(), int(time())
        load = RawModel(value=val, date=dt)
        event = await agent.put(load)
        assert tumbling_table[dt]
        page_view = PageView(id='1', user='test')
        page_view_2 = PageView(id='1', user='test2')
        await agent.put(page_view)

        # windowed table: we select window relative to the current event
        assert page_views['1'] == 1

        await agent.put(page_view_2)
        assert page_views['1'] == 2
