# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
from typing import Any, Dict, Optional
import warnings

from swh.core import config
from swh.core.api import RPCServerApp
from swh.core.api import encode_data_server as encode_data
from swh.core.api import error_handler
from swh.indexer.storage import INDEXER_CFG_KEY, get_indexer_storage
from swh.indexer.storage.exc import IndexerStorageArgumentException
from swh.indexer.storage.interface import IndexerStorageInterface

from .serializers import DECODERS, ENCODERS


def get_storage():
    global storage
    if not storage:
        storage = get_indexer_storage(**app.config[INDEXER_CFG_KEY])

    return storage


class IndexerStorageServerApp(RPCServerApp):
    extra_type_decoders = DECODERS
    extra_type_encoders = ENCODERS


app = IndexerStorageServerApp(
    __name__, backend_class=IndexerStorageInterface, backend_factory=get_storage
)
storage = None


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


app.setup_psycopg2_errorhandlers()


@app.errorhandler(IndexerStorageArgumentException)
def argument_error_handler(exception):
    return error_handler(exception, encode_data, status_code=400)


@app.route("/")
def index():
    return "SWH Indexer Storage API server"


api_cfg = None


def load_and_check_config(
    config_path: Optional[str],
) -> Dict[str, Any]:
    """Check the minimal configuration is set to run the api or raise an
       error explanation.

    Args:
        config_path: Path to the configuration file to load
        cls: backend class (as declared in swh.indexer.classes entry point)

    Raises:
        Error if the setup is not as expected

    Returns:
        configuration as a dict

    """
    if not config_path:
        raise EnvironmentError("Configuration file must be defined")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file {config_path} does not exist")

    cfg = config.read(config_path)
    if "indexer.storage" in cfg:
        warnings.warn(
            "The 'indexer.storage' configuration section should be renamed "
            "as 'indexer_storage'",
            DeprecationWarning,
        )
        cfg["indexer_storage"] = cfg.pop("indexer.storage")
    if "indexer_storage" not in cfg:
        raise KeyError("Missing '%indexer_storage' configuration")

    return cfg


def make_app_from_configfile():
    """Run the WSGI app from the webserver, loading the configuration from
    a configuration file.

    SWH_CONFIG_FILENAME environment variable defines the
    configuration path to load.

    """
    global api_cfg
    if not api_cfg:
        config_path = os.environ.get("SWH_CONFIG_FILENAME")
        api_cfg = load_and_check_config(config_path)
        app.config.update(api_cfg)
    handler = logging.StreamHandler()
    app.logger.addHandler(handler)
    return app


if __name__ == "__main__":
    print("Deprecated. Use swh-indexer")
