from requests import Session

from src.log import logger
from src.tarantool.models import UploadTable

t1 = UploadTable(name="a", columns=["f_1", "f_2"], rows=[[1, 2]])

t2 = UploadTable(name="b", columns=["f_1", "f_2"], rows=[[1, 2]])

sess = Session()

endpoint = "http://localhost:8000/upload"
sid = sess.post(f"{endpoint}/start/").text
logger.debug(sid)

for t in [t1, t2]:
    sess.post(f"{endpoint}/clear-table/", json={"session_id": sid, "table_name": t.name})
    sess.post(f"{endpoint}/table/", json={"session_id": sid, "table": t.model_dump()})

sid = sess.post(f"{endpoint}/complete/", json=sid)
sess.close()
