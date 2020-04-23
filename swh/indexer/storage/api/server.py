# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import logging

from swh.core import config
from swh.core.api import RPCServerApp, error_handler, encode_data_server as encode_data
from swh.indexer.storage import get_indexer_storage, INDEXER_CFG_KEY
from swh.indexer.storage.exc import IndexerStorageArgumentException
from swh.indexer.storage.interface import IndexerStorageInterface


def get_storage():
    global storage
    if not storage:
        storage = get_indexer_storage(**app.config[INDEXER_CFG_KEY])

    return storage


app = RPCServerApp(
    __name__, backend_class=IndexerStorageInterface, backend_factory=get_storage
)
storage = None


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


@app.errorhandler(IndexerStorageArgumentException)
def argument_error_handler(exception):
    return error_handler(exception, encode_data, status_code=400)


@app.route("/")
def index():
    return "SWH Indexer Storage API server"


api_cfg = None


def load_and_check_config(config_file, type="local"):
    """Check the minimal configuration is set to run the api or raise an
       error explanation.

    Args:
        config_file (str): Path to the configuration file to load
        type (str): configuration type. For 'local' type, more
                    checks are done.

    Raises:
        Error if the setup is not as expected

    Returns:
        configuration as a dict

    """
    if not config_file:
        raise EnvironmentError("Configuration file must be defined")

    if not os.path.exists(config_file):
        raise FileNotFoundError("Configuration file %s does not exist" % (config_file,))

    cfg = config.read(config_file)
    if "indexer_storage" not in cfg:
        raise KeyError("Missing '%indexer_storage' configuration")

    if type == "local":
        vcfg = cfg["indexer_storage"]
        cls = vcfg.get("cls")
        if cls != "local":
            raise ValueError(
                "The indexer_storage backend can only be started with a "
                "'local' configuration"
            )

        args = vcfg["args"]
        if not args.get("db"):
            raise ValueError("Invalid configuration; missing 'db' config entry")

    return cfg


def make_app_from_configfile():
    """Run the WSGI app from the webserver, loading the configuration from
       a configuration file.

       SWH_CONFIG_FILENAME environment variable defines the
       configuration path to load.

    """
    global api_cfg
    if not api_cfg:
        config_file = os.environ.get("SWH_CONFIG_FILENAME")
        api_cfg = load_and_check_config(config_file)
        app.config.update(api_cfg)
    handler = logging.StreamHandler()
    app.logger.addHandler(handler)
    return app


if __name__ == "__main__":
    print("Deprecated. Use swh-indexer")
