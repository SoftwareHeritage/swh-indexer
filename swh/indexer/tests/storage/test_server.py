# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import yaml

from swh.indexer.storage.api.server import load_and_check_config


def prepare_config_file(tmpdir, content, name="config.yml") -> str:
    """Prepare configuration file in `$tmpdir/name` with content `content`.

    Args:
        tmpdir (LocalPath): root directory
        content (str/dict): Content of the file either as string or as a dict.
                            If a dict, converts the dict into a yaml string.
        name (str): configuration filename

    Returns
        path (str) of the configuration file prepared.

    """
    config_path = tmpdir / name
    if isinstance(content, dict):  # convert if needed
        content = yaml.dump(content)
    config_path.write_text(content, encoding="utf-8")
    # pytest on python3.5 does not support LocalPath manipulation, so
    # convert path to string
    return str(config_path)


@pytest.mark.parametrize("config_path", [None, ""])
def test_load_and_check_config_no_configuration(config_path) -> None:
    """Irrelevant configuration file path raises"""
    with pytest.raises(EnvironmentError, match="Configuration file must be defined"):
        load_and_check_config(config_path)


def test_load_and_check_inexistent_config_path() -> None:
    """Inexistent configuration file raises"""
    config_path = "/indexer/inexistent/config.yml"
    expected_error = f"Configuration file {config_path} does not exist"
    with pytest.raises(FileNotFoundError, match=expected_error):
        load_and_check_config(config_path)


def test_load_and_check_config_wrong_configuration(tmpdir) -> None:
    """Wrong configuration raises"""
    config_path = prepare_config_file(tmpdir, "something: useless")
    with pytest.raises(KeyError, match="Missing '%indexer_storage' configuration"):
        load_and_check_config(config_path)


def test_load_and_check_config_remote_config_fine(tmpdir) -> None:
    """'Remote configuration is fine (when changing the default type)"""
    config = {"indexer_storage": {"cls": "remote"}}
    config_path = prepare_config_file(tmpdir, config)
    cfg = load_and_check_config(config_path)

    assert cfg == config


def test_load_and_check_config_local_config_fine(tmpdir) -> None:
    """'Complete 'postgresql' configuration is fine"""
    config = {
        "indexer_storage": {
            "cls": "postgresql",
            "db": "db",
        }
    }
    config_path = prepare_config_file(tmpdir, config)
    cfg = load_and_check_config(config_path)
    assert cfg == config


def test_load_and_check_config_deprecated(tmpdir) -> None:
    """'Complete 'local' configuration is fine"""
    config = {
        "indexer.storage": {
            "cls": "postgresql",
            "db": "db",
        }
    }
    config_path = prepare_config_file(tmpdir, config)
    with pytest.warns(DeprecationWarning):
        cfg = load_and_check_config(config_path)
        assert "indexer_storage" in cfg
