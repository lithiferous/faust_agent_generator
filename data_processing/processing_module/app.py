import faust

from settings import APP_ID, KAFKA_BOOTSTRAP_SERVER, LOGGING
from logging.config import dictConfig

app = faust.App(
    version=1,
    autodiscover=True,
    origin='processing_module',
    id=APP_ID,
    broker=KAFKA_BOOTSTRAP_SERVER,
    logging_config=dictConfig(LOGGING),
)


def main() -> None:
    app.main()
