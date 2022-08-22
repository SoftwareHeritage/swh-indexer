# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.indexer.metadata_dictionary import MAPPINGS


def test_compute_metadata_podspec():
    raw_content = b"""Pod::Spec.new do |spec|
  spec.name          = 'Reachability'
  spec.version       = '3.1.0'
  spec.license       = { :type => 'BSD' }
  spec.homepage      = 'https://github.com/tonymillion/Reachability'
  spec.authors       = { 'Tony Million' => 'tonymillion@gmail.com' }
  spec.summary       = 'ARC and GCD Compatible Reachability Class for iOS and OS X.'
  spec.source        = { :git => 'https://github.com/tonymillion/Reachability.git' }
  spec.module_name   = 'Rich'
  spec.swift_version = '4.0'

  spec.ios.deployment_target  = '9.0'
  spec.osx.deployment_target  = '10.10'

  spec.source_files       = 'Reachability/common/*.swift'
  spec.ios.source_files   = 'Reachability/ios/*.swift', 'Reachability/extensions/*.swift'
  spec.osx.source_files   = 'Reachability/osx/*.swift'

  spec.framework      = 'SystemConfiguration'
  spec.ios.framework  = 'UIKit'
  spec.osx.framework  = 'AppKit'

  spec.dependency 'SomeOtherPod'
end"""
    result = MAPPINGS["PodspecMapping"]().translate(raw_content)
    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [
            {"type": "Person", "name": "Tony Million", "email": "tonymillion@gmail.com"}
        ],
        "description": "ARC and GCD Compatible Reachability Class for iOS and OS X.",
        "url": "https://github.com/tonymillion/Reachability",
        "codeRepository": "https://github.com/tonymillion/Reachability.git",
        "name": "Reachability",
        "softwareVersion": "3.1.0",
    }

    assert result == expected


def test_parse_enum():
    raw_content = """{
        :git => 'https://github.com/tensorflow/tensorflow.git',
        :commit => 'd8ce9f9c301d021a69953134185ab728c1c248d3'
        }
    """
    expected = {
        ":git": "https://github.com/tensorflow/tensorflow.git",
        ":commit": "d8ce9f9c301d021a69953134185ab728c1c248d3",
    }

    result = MAPPINGS["PodspecMapping"]().parse_enum(raw_content)

    assert result == expected
