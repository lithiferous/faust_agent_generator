import faust
import os
from shutil import copyfile

from simple_settings import settings
from logging.config import dictConfig


def generate_agents():
    def create_module(path):
        def create_dir(dst_dir):
            if not os.path.exists(dst_dir):
                os.makedirs(dst_dir)

        create_dir(path)
        open(f"{path}/" + "__init__.py", 'a').close()

    cwd = os.getcwd() + '/processing_module'
    src_dir = cwd + '/tests'
    files = os.listdir(src_dir)
    for file in files:
        _dir = file.replace(".py", "")
        create_module(cwd + f"/{_dir}")
        copyfile(src_dir + f"/{file}", cwd + f"/{_dir}" + f"/{file}")


app = faust.App(
    version=1,
    autodiscover=True,
    origin='processing_module',
    id="1",
    broker=settings.KAFKA_BOOTSTRAP_SERVER,
    logging_config=dictConfig(settings.LOGGING),
)


def main() -> None:
    generate_agents()
    app.main()
