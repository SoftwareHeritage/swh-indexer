# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


class ReportableError(RuntimeError):
    """A specific exception raised by Indexer implementation when they want such
    exception to be trapped and reported without stopping the overall indexation
    process.

    """

    def __str__(self):
        return self.args[0]
