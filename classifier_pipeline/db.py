from pydantic import BaseModel, PrivateAttr
from typing import Any, Optional, NamedTuple, Dict
from rethinkdb import RethinkDB


class Table(NamedTuple):
    rethink: Any
    conn: Any
    db: Any
    table: Any


class Db(BaseModel):
    """
    Represents the data store

    Class attributes
    ----------

    Attributes
    ----------
    host : str = 'localhost'
        Database host
    port : int = 28015
        Client drivers port
    db : str = 'classifier'
        Database name
    user : Optional[str]
        Db user name
    password : Optional[str]
        Db password


    Methods
    ----------
    access_table() -> Table:
        Provide for a database table if it doesn't already exist
    set(table_name: str, data: Dict[str, Any]) -> Dict[str, int]
        Either insert the document or replace it if the id exists and return status flags
        {
            "deleted": int,
            "errors": int,
            "inserted": int,
            "replaced": int,
            "skipped": int,
            "unchanged": int
        }
        see https://rethinkdb.com/api/python/insert; https://rethinkdb.com/api/python/replace
    """

    _r: Any = PrivateAttr()
    _conn: Any = PrivateAttr()
    _db: Any = PrivateAttr()

    host: str = 'localhost'
    port: int = 28015
    db_name: str = 'classifier'
    user: Optional[str] = 'admin'
    password: Optional[str] = ''

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._r = RethinkDB()
        self._db = None
        self._conn = None

    def _connect(self):
        conn = None
        if self._conn is not None:
            conn = self._conn
        else:
            self._conn = self._r.connect(host=self.host, port=self.port, user=self.user, password=self.password)
            conn = self._conn
        return conn

    def _guarantee_db(self):
        db = None
        conn = self._connect()
        if self._db is not None:
            db = self._db
        else:
            dbs = self._r.db_list().run(conn)
            if self.db_name not in dbs:
                self._r.db_create(self.db_name).run(conn)
            self._db = self._r.db(self.db_name)
            db = self._db
        return db

    def _guarantee_table(self, table_name: str) -> Table:
        table = None
        conn = self._connect()
        db = self._guarantee_db()
        tables = db.table_list().run(conn)

        if table_name not in tables:
            db.table_create(table_name).run(conn)
        table = db.table(table_name)
        return Table(self._r, conn, db, table)

    def access_table(self, table_name: str) -> Table:
        return self._guarantee_table(table_name)

    def set(self, table_name: str, data: Dict[str, Any]) -> Dict[str, int]:
        set_result = None
        exists = False
        id = None
        _, conn, _, table = self._guarantee_table(table_name)

        if 'id' in data:
            id = data['id']
            result = table.get(id).run(conn)
            if result is not None:
                exists = True
        if exists:
            set_result = table.get(id).replace(data).run(conn)
        else:
            set_result = table.insert(data).run(conn)
        return set_result
