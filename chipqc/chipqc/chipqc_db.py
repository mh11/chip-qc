from contextlib import contextmanager
import sqlite3 as lite
import os.path
import sys

#http://stackoverflow.com/questions/3783238/python-database-connection-close
@contextmanager
def open_db_connection(connection_string):
    connection = lite.connect(connection_string)
    cursor = connection.cursor()
    try:
        yield cursor
    except lite.DatabaseError as err:
        error, = err.args
        sys.stderr.write(error)
        # cursor.execute("ROLLBACK")
        raise err
    finally:
        connection.commit()
        cursor.close()
        connection.close()

class ChipQcDbSqlite:

    path = None
    new_db = None
    key_id = {}

    def __init__(self,path):
        self.path = path
        self.new_db = (not os.path.isfile(self.path))
        self.initDb()

    def initDb(self):
        if self.new_db:
            with open_db_connection(self.path) as cur:
                cur.execute("CREATE TABLE data_file(did INT UNIQUE, external_id TEXT UNIQUE, data_file TEXT UNIQUE)")
                cur.execute("CREATE TABLE data_key(kid INT UNIQUE, key TEXT)")
                cur.execute("CREATE TABLE data_annotation(did INT, kid INT,value TEXT)")
                cur.execute("CREATE TABLE filter(did INT UNIQUE,f_file_path TEXT, status TEXT, started TEXT, finished TEXT, exit_code TEXT,out TEXT,err TEXT)")
                cur.execute("CREATE TABLE correlation(corr_id INT,did_a INT, did_b INT, status TEXT, started TEXT, finished TEXT, exit_code TEXT, out TEXT, err TEXT,run_count INT DEFAULT 0) ")

    def getMaxDataId(self):
        with open_db_connection(self.path) as cur:
            cur.execute("SELECT MAX(did) FROM data_file")
            currMaxId = cur.fetchone()[0]
            if currMaxId == None:
                currMaxId = 0
            return currMaxId

    # did,ext,data_file
    def addDataFileAll(self,valueList):
        dQuery = "INSERT INTO data_file (did,external_id,data_file) VALUES(?,?,?)"
        with open_db_connection(self.path) as cur:
            cur.executemany(dQuery, valueList)

    def addAnnotationAll(self,valueList):
        insertList = [(id,self._getKeyId(key),val) for (id,key,val) in valueList]
        with open_db_connection(self.path) as cur:
            cur.executemany("INSERT INTO data_annotation (did,kid,value) VALUES (?,?,?)",insertList)

    def _getKeyId(self,key):
        if key not in self.key_id.keys():
            with open_db_connection(self.path) as cur:
                cur.execute("SELECT kid FROM data_key WHERE key = ?",[key])
                res = cur.fetchall()
                if len(res) > 0:
                    kid = res[0][0]
                    self.key_id[key] = kid
                else:
                    cur.execute("SELECT max(kid) FROM data_key")
                    res = cur.fetchall()
                    maxId = 0
                    if len(res) > 0:
                        maxId = res[0][0]
                        if maxId is None:
                            maxId = 0
                    maxId = maxId + 1
                    cur.execute("INSERT INTO data_key(kid,key) VALUES (?,?)", [maxId,key])
                    self.key_id[key] = maxId
        return self.key_id[key]

    def getFileCount(self):
        with open_db_connection(self.path) as cur:
            cur.execute("SELECT count(*) FROM data_file")
            return cur.fetchone()[0]

    # did, external_id, data_file
    def getFiles(self):
        with open_db_connection(self.path) as cur:
            cur.execute("SELECT did, external_id, data_file FROM data_file")
            return cur.fetchall()

    # did,file_path,status
    def getFilesFiltered(self):
        with open_db_connection(self.path) as cur:
            cur.execute("SELECT did, f_file_path, status FROM filter")
            return cur.fetchall()

    ## Requires tubles of (did,file_path,status)
    def addFileFilteredAll(self, valueList):
        with open_db_connection(self.path) as cur:
#            cur.execute("INSERT INTO filter (did,f_file_path,status) SELECT did,data_file,'init' FROM data_file d WHERE d.did not in (SELECT did from filter)")
            cur.executemany("INSERT INTO filter (did,f_file_path,status) VALUES (?,?,?)",valueList)

    # corr_id, did_a, did_b
    def getCorrelationIds(self):
        with open_db_connection(self.path) as cur:
            cur.execute("SELECT corr_id,did_a,did_b FROM correlation")
            return cur.fetchall()

    # corr_id, did_a, did_b
    def addCorrelationInitAll(self, values):
        with open_db_connection(self.path) as cur:
            cur.executemany("INSERT INTO correlation (corr_id,did_a, did_b,status,run_count) VALUES (?,?,?,'init',0)",values)

    def getCorrelationCount(self):
        with open_db_connection(self.path) as cur:
            cur.execute("SELECT count(*) from correlation")
            (cnt,) = cur.fetchone()
            return cnt
