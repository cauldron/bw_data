import json
import pickle
import os
from pathlib import Path

from peewee import BlobField, SqliteDatabase, TextField, PostgresqlDatabase

from bw2data.logs import stdout_feedback_logger


class PickleField(BlobField):
    def db_value(self, value):
        return super(PickleField, self).db_value(pickle.dumps(value, protocol=4))

    def python_value(self, value):
        return pickle.loads(bytes(value))


class SubstitutableDatabase:
    def __init__(self, filepath: Path, tables: list):
        self._filepath = filepath
        self._tables = tables
        self._database = self._create_database()

    def check_postgres(self) -> dict:
        if not os.environ.get('BW_DATA_POSTGRES'):
            return {}
        return {
            "db": os.environ['BW_DATA_POSTGRES_DATABASE'],
            "user": os.environ.get('BW_DATA_POSTGRES_USER'),
            "password": os.environ.get('BW_DATA_POSTGRES_PASSWORD'),
            "port": int(os.environ.get('BW_DATA_POSTGRES_PORT', 5432)),
            "url": os.environ.get('BW_DATA_POSTGRES_URL', "localhost"),
        }

    def _create_database(self):
        pg_config = self.check_postgres()
        if not pg_config:
            db = SqliteDatabase(self._filepath)
            stdout_feedback_logger.info("Using SQLite driver")
        else:
            db = PostgresqlDatabase(
                pg_config['db'],
                user=pg_config['user'],
                password=pg_config['password'],
                host=pg_config['url'],
                port=pg_config['port'],
            )
            stdout_feedback_logger.info(
                f"Using Postgres driver with database {pg_config['db']} and user {pg_config['user']}"
            )
        for model in self._tables:
            model.bind(db, bind_refs=False, bind_backrefs=False)
        db.connect()
        db.create_tables(self._tables)
        return db

    @property
    def db(self):
        return self._database

    def change_path(self, filepath):
        self.db.close()
        self._filepath = filepath
        self._database = self._create_database()

    def atomic(self):
        return self.db.atomic()

    def execute_sql(self, *args, **kwargs):
        return self.db.execute_sql(*args, **kwargs)

    def transaction(self):
        return self.db.transaction()

    def vacuum(self):
        stdout_feedback_logger.info("Vacuuming database ")
        self.execute_sql("VACUUM;")


class JSONField(TextField):
    """Simpler JSON field that doesn't support advanced querying and is human-readable"""

    def db_value(self, value):
        return super().db_value(
            json.dumps(
                value,
                ensure_ascii=False,
                indent=2,
                default=lambda x: x.isoformat() if hasattr(x, "isoformat") else x,
            )
        )

    def python_value(self, value):
        return json.loads(value)


class TupleJSONField(JSONField):
    def python_value(self, value):
        if value is None:
            return None
        data = json.loads(value)
        if isinstance(data, list):
            data = tuple(data)
        return data
