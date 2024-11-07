from src.tarantool.models import UploadTable
from src.tarantool.repository import Statement


class UploadService:
    @staticmethod
    def upload_table(table: UploadTable):
        stmt = Statement()
        # table.rows = pd.DataFrame(table.rows).replace("", None).to_numpy().tolist()
        stmt.upsert_copy_from(table)
