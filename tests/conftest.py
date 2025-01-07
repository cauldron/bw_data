"""Fixtures for bw2data"""

import sqlite3

import pytest

from bw2data.tests import bw_test_fixture

sqlite3.enable_callback_tracebacks(True)


@pytest.fixture
def num_revisions():
    def _num_revisions(projects) -> int:
        return len(
            [
                fp
                for fp in (projects.dataset.dir / "revisions").iterdir()
                if fp.stem.lower() != "head" and fp.is_file()
            ]
        )

    return _num_revisions
