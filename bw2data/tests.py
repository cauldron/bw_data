import atexit
import os
import random
import shutil
import string
import tempfile
import unittest
from pathlib import Path

try:
    from testcontainers.postgres import PostgresContainer
except ImportError:
    PostgresContainer = None

import pytest

from bw2data import config, databases, geomapping, methods
from bw2data.project import projects


class BW2DataTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        config.dont_warn = True
        config.is_test = True
        tempdir = Path(tempfile.mkdtemp())
        project_name = "".join(random.choices(string.ascii_lowercase, k=18))
        projects.change_base_directories(
            base_dir=tempdir,
            base_logs_dir=tempdir,
            project_name=project_name,
            update=False,
        )
        projects._is_temp_dir = True
        self.extra_setup()

    def extra_setup(self):
        pass

    def test_setup_clean(self):
        self.assertEqual(list(databases), [])
        self.assertEqual(list(methods), [])
        self.assertEqual(len(geomapping), 1)  # GLO
        self.assertTrue("GLO" in geomapping)
        self.assertEqual(len(projects), 1)  # Default project
        self.assertTrue("default" not in projects)


@pytest.fixture(params=[True, False])
def bw_test_fixture(request, tmp_path) -> None:
    config.dont_warn = True
    config.is_test = True
    project_name = "".join(random.choices(string.ascii_lowercase, k=18))
    if request.param and PostgresContainer:
        postgres = PostgresContainer("postgres:16")
        postgres.start()

        def remove_container():
            postgres.stop()

            del os.environ['BW_DATA_POSTGRES_URL']
            del os.environ['BW_DATA_POSTGRES_PORT']
            del os.environ['BW_DATA_POSTGRES_USER']
            del os.environ['BW_DATA_POSTGRES_PASSWORD']
            del os.environ['BW_DATA_POSTGRES_DATABASE']
            del os.environ['BW_DATA_POSTGRES']

        request.addfinalizer(remove_container)

        os.environ['BW_DATA_POSTGRES'] = "1"
        os.environ['BW_DATA_POSTGRES_URL'] = postgres.get_container_host_ip()
        os.environ['BW_DATA_POSTGRES_PORT'] = postgres.get_exposed_port(5432)
        os.environ['BW_DATA_POSTGRES_USER'] = postgres.username
        os.environ['BW_DATA_POSTGRES_PASSWORD'] = postgres.password
        os.environ['BW_DATA_POSTGRES_DATABASE'] = postgres.dbname
    else:
        os.environ['BW_DATA_POSTGRES'] = ""

        def remove_envvar():
            del os.environ['BW_DATA_POSTGRES']

        request.addfinalizer(remove_envvar)

    projects.change_base_directories(
        base_dir=tmp_path, base_logs_dir=tmp_path, project_name=project_name, update=False
    )
    projects._is_temp_dir = True
