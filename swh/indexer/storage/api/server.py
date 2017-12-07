# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import click

from flask import g, request

from swh.core import config
from swh.core.api import (SWHServerAPIApp, decode_request,
                          error_handler,
                          encode_data_server as encode_data)
from swh.indexer import get_indexer_storage, INDEXER_CFG_KEY


DEFAULT_CONFIG_PATH = 'storage/indexer'
DEFAULT_CONFIG = {
    INDEXER_CFG_KEY: ('dict', {
        'cls': 'local',
        'args': {
            'db': 'dbname=softwareheritage-indexer-dev',
        },
    })
}


app = SWHServerAPIApp(__name__)


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


@app.before_request
def before_request():
    g.storage = get_indexer_storage(**app.config[INDEXER_CFG_KEY])


@app.route('/')
def index():
    return 'SWH Indexer Storage API server'


@app.route('/check_config', methods=['POST'])
def check_config():
    return encode_data(g.storage.check_config(**decode_request(request)))


@app.route('/content_mimetype/add', methods=['POST'])
def content_mimetype_add():
    return encode_data(
        g.storage.content_mimetype_add(**decode_request(request)))


@app.route('/content_mimetype/missing', methods=['POST'])
def content_mimetype_missing():
    return encode_data(
        g.storage.content_mimetype_missing(**decode_request(request)))


@app.route('/content_mimetype', methods=['POST'])
def content_mimetype_get():
    return encode_data(
        g.storage.content_mimetype_get(**decode_request(request)))


@app.route('/content_language/add', methods=['POST'])
def content_language_add():
    return encode_data(
        g.storage.content_language_add(**decode_request(request)))


@app.route('/content_language/missing', methods=['POST'])
def content_language_missing():
    return encode_data(
        g.storage.content_language_missing(**decode_request(request)))


@app.route('/content_language', methods=['POST'])
def content_language_get():
    return encode_data(
        g.storage.content_language_get(**decode_request(request)))


@app.route('/content/ctags/add', methods=['POST'])
def content_ctags_add():
    return encode_data(
        g.storage.content_ctags_add(**decode_request(request)))


@app.route('/content/ctags/search', methods=['POST'])
def content_ctags_search():
    return encode_data(
        g.storage.content_ctags_search(**decode_request(request)))


@app.route('/content/ctags/missing', methods=['POST'])
def content_ctags_missing():
    return encode_data(
        g.storage.content_ctags_missing(**decode_request(request)))


@app.route('/content/ctags', methods=['POST'])
def content_ctags_get():
    return encode_data(
        g.storage.content_ctags_get(**decode_request(request)))


@app.route('/content/fossology_license/add', methods=['POST'])
def content_fossology_license_add():
    return encode_data(
        g.storage.content_fossology_license_add(**decode_request(request)))


@app.route('/content/fossology_license', methods=['POST'])
def content_fossology_license_get():
    return encode_data(
        g.storage.content_fossology_license_get(**decode_request(request)))


@app.route('/indexer_configuration/data', methods=['POST'])
def indexer_configuration_get():
    return encode_data(g.storage.indexer_configuration_get(
        **decode_request(request)))


@app.route('/indexer_configuration/add', methods=['POST'])
def indexer_configuration_add():
    return encode_data(g.storage.indexer_configuration_add(
        **decode_request(request)))


@app.route('/content_metadata/add', methods=['POST'])
def content_metadata_add():
    return encode_data(
        g.storage.content_metadata_add(**decode_request(request)))


@app.route('/content_metadata/missing', methods=['POST'])
def content_metadata_missing():
    return encode_data(
        g.storage.content_metadata_missing(**decode_request(request)))


@app.route('/content_metadata', methods=['POST'])
def content_metadata_get():
    return encode_data(
        g.storage.content_metadata_get(**decode_request(request)))


@app.route('/revision_metadata/add', methods=['POST'])
def revision_metadata_add():
    return encode_data(
        g.storage.revision_metadata_add(**decode_request(request)))


@app.route('/revision_metadata/missing', methods=['POST'])
def revision_metadata_missing():
    return encode_data(
        g.storage.revision_metadata_missing(**decode_request(request)))


@app.route('/revision_metadata', methods=['POST'])
def revision_metadata_get():
    return encode_data(
        g.storage.revision_metadata_get(**decode_request(request)))


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
