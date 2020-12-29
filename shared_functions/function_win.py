
from random import random
from datetime import timedelta
import faust

app = faust.App('windowing', broker='kafka://localhost:9092')


class Model(faust.Record, serializer='json'):
    random: float


TOPIC = 'tumbling_topic'

tumbling_topic = app.topic(TOPIC, value_type=Model)
tumbling_table = app.Table(
    'tumbling_table',
    default=int).tumbling(10, expires=timedelta(minutes=10))


@app.agent(tumbling_topic)
async def print_windowed_events(stream):
    async for _ in stream: # noqa
        tumbling_table['counter'] += 1

        value = tumbling_table['counter']

        print('-- New Event (every 2 secs) written to tumbling(10) --')
        print(f'now() should go from 1 to 5: {value.now()}')
        print(f'current() should go from 1 to 5: {value.current()}')
        print(f'value() should go from 1 to 5: {value.value()}')
        print('delta(30) should start at 0 and after 40 secs be 5: '
              f'{value.delta(30)}')


@app.timer(2.0, on_leader=True)
async def publish_every_2secs():
    msg = Model(random=round(random(), 2))
    await tumbling_topic.send(value=msg)


if __name__ == '__main__':
    app.main()
