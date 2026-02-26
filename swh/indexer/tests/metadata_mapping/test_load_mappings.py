# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from importlib_metadata import EntryPoint
import pytest

from swh.indexer.metadata_mapping import load_mappings
from swh.indexer.metadata_mapping.base import BaseExtrinsicMapping, BaseIntrinsicMapping


class TestMapping0(BaseExtrinsicMapping):
    pass


class TestMapping1(BaseIntrinsicMapping):
    pass


class TestMapping2(BaseExtrinsicMapping):
    pass


class TestMapping3(BaseIntrinsicMapping):
    pass


class TestMapping4(BaseExtrinsicMapping):
    pass


def test_load_mapping(mocker):
    mock = mocker.patch("backports.entry_points_selectable.entry_points")
    mock.return_value = [
        EntryPoint(
            name=f"Mapping{i}",
            value=f"swh.indexer.tests.metadata_mapping.test_load_mappings:TestMapping{i}",
            group="swh.indexer.metadata_mappings",
        )
        for i in range(5)
    ]
    mappings = load_mappings()
    assert len(mappings) == 5


def test_load_mapping_multimap_selected_before(mocker):
    selected_mapping = "swh.indexer.tests.metadata_mapping.test_load_mappings"
    overloaded_mapping = "swh.indexer.metadata_mapping.test_load_mappings"

    mock = mocker.patch("backports.entry_points_selectable.entry_points")
    mock.return_value = [
        EntryPoint(
            name=f"Mapping{i}",
            value=f"{selected_mapping}:TestMapping{i}",
            group="swh.indexer.metadata_mappings",
        )
        for i in range(5)
    ] + [
        EntryPoint(
            name=f"Mapping{i}",
            value=f"{overloaded_mapping}:TestMapping{i}",
            group="swh.indexer.metadata_mappings",
        )
        for i in range(5)
    ]

    # when the same mappings are declared both from
    # swh.indexer.metadata_mappings and some "other" location (here
    # swh.indexer.tests) the "other" module takes precedence to ease migration
    mappings = load_mappings()
    assert len(mappings) == 5
    assert all([cls.__module__ == selected_mapping for cls in mappings.values()])


def test_load_mapping_multimap_selected_after(mocker):
    selected_mapping = "swh.indexer.tests.metadata_mapping.test_load_mappings"
    overloaded_mapping = "swh.indexer.metadata_mapping.test_load_mappings"

    mock = mocker.patch("backports.entry_points_selectable.entry_points")
    mock.return_value = [
        EntryPoint(
            name=f"Mapping{i}",
            value=f"{overloaded_mapping}:TestMapping{i}",
            group="swh.indexer.metadata_mappings",
        )
        for i in range(5)
    ] + [
        EntryPoint(
            name=f"Mapping{i}",
            value=f"{selected_mapping}:TestMapping{i}",
            group="swh.indexer.metadata_mappings",
        )
        for i in range(5)
    ]

    # same as above (but ordered differently)
    mappings = load_mappings()
    assert len(mappings) == 5
    assert all([cls.__module__ == selected_mapping for cls in mappings.values()])


def test_load_mapping_multimap_fail(mocker):
    mock = mocker.patch("backports.entry_points_selectable.entry_points")
    mock.return_value = [
        EntryPoint(
            name=f"Mapping{i}",
            value=f"swh.pkg1:TestMapping{i}",
            group="swh.indexer.metadata_mappings",
        )
        for i in range(5)
    ] + [
        EntryPoint(
            name=f"Mapping{i}",
            value=f"swh.pkg2:TestMapping{i}",
            group="swh.indexer.metadata_mappings",
        )
        for i in range(5)
    ]
    # will fail because mappings from non-swh-indexer packages are conflicting...
    with pytest.raises(
        EnvironmentError,
        match=(
            "The metadata mapping Mapping0 from swh.pkg2 "
            "is conflicting with swh.pkg1.Mapping0"
        ),
    ):
        load_mappings()
