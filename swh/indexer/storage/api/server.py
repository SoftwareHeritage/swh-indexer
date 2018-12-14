# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import click

from swh.core import config
from swh.core.api import (SWHServerAPIApp, error_handler,
                          encode_data_server as encode_data)
from swh.indexer.storage import (
    get_indexer_storage, INDEXER_CFG_KEY, IndexerStorage
)


DEFAULT_CONFIG_PATH = 'storage/indexer'
DEFAULT_CONFIG = {
    INDEXER_CFG_KEY: ('dict', {
        'cls': 'local',
        'args': {
            'db': 'dbname=softwareheritage-indexer-dev',
        },
    })
}


def get_storage():
    global storage
    if not storage:
        storage = get_indexer_storage(**app.config[INDEXER_CFG_KEY])

    return storage


app = SWHServerAPIApp(__name__,
                      backend_class=IndexerStorage,
                      backend_factory=get_storage)
storage = None


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


@app.route('/')
def index():
    return 'SWH Indexer Storage API server'


def run_from_webserver(environ, start_response,
                       config_path=DEFAULT_CONFIG_PATH):
    """Run the WSGI app from the webserver, loading the configuration."""
    cfg = config.load_named_config(config_path, DEFAULT_CONFIG)
    app.config.update(cfg)
    handler = logging.StreamHandler()
    app.logger.addHandler(handler)
    return app(environ, start_response)


@click.command()
@click.argument('config-path', required=1)
@click.option('--host', default='0.0.0.0', help="Host to run the server")
@click.option('--port', default=5007, type=click.INT,
              help="Binding port of the server")
@click.option('--debug/--nodebug', default=True,
              help="Indicates if the server should run in debug mode")
def launch(config_path, host, port, debug):
    app.config.update(config.read(config_path, DEFAULT_CONFIG))
    app.run(host, port=int(port), debug=bool(debug))


if __name__ == '__main__':
    launch()
