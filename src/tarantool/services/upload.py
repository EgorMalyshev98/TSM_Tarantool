from src.tarantool.models import UploadTable
from src.tarantool.repository import Statement


class UploadService:
    def upload_table(self: UploadTable):
        stmt = Statement()
        stmt.upsert_copy_from(self)
