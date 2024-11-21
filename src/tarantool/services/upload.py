from sqlalchemy.engine.interfaces import DBAPICursor

from src.tarantool.models import UploadTable
from src.tarantool.repository import Statement


class UploadService:
    @staticmethod
    def upload_table(table: UploadTable, cursor: DBAPICursor):
        stmt = Statement()
        stmt.batch_insert(table, cursor)

    @staticmethod
    def clear_table(table_name: str, cursor: DBAPICursor):
        stmt = Statement()
        stmt.truncate(table_name, cursor)
