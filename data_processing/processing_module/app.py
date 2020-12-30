import faust
import os
from shutil import copyfile

import settings
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
    _files = [file for file in files if not file.startswith("_")]
    for file in _files:
        _dir = file.replace(".py", "")
        create_module(cwd + f"/{_dir}")
        copyfile(src_dir + f"/{file}", cwd + f"/{_dir}" + f"/{file}")


app = faust.App(
    version=1,
    autodiscover=True,
    origin='processing_module',
    id=settings.APP_ID,
    broker=settings.KAFKA_BOOTSTRAP_SERVER,
    logging_config=dictConfig(settings.LOGGING),
)


def main() -> None:
    generate_agents()
    app.main()
