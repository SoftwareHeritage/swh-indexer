import pathlib
import sys

from swh.docs.sphinx.conf import *  # NoQA

sys.path.append(str(pathlib.Path(__file__).parent / "_ext"))
extensions += ["ld_properties"]
