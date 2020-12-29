from datetime import datetime, timedelta
import faust

class RawModel(faust.Record):
    date: datetime
    value: float


class AggModelA(faust.Record):
    date: datetime
    count: int
    mean: float
