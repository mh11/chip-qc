from contextlib import contextmanager
import sqlite3 as lite
import os.path
import sys

#http://stackoverflow.com/questions/3783238/python-database-connection-close
@contextmanager
def open_db_connection(connection_string):
    connection = lite.connect(connection_string, timeout=30.0)
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

####################
## Files
    def getFileCount(self):
        with open_db_connection(self.path) as cur:
            cur.execute("SELECT count(*) FROM data_file")
            return cur.fetchone()[0]

    # did, external_id, data_file
    def getFiles(self):
        with open_db_connection(self.path) as cur:
            cur.execute("SELECT did, external_id, data_file FROM data_file")
            return cur.fetchall()

####################
## Filter
    # did,file_path,status
    def getFilesFiltered(self):
        with open_db_connection(self.path) as cur:
            cur.execute("SELECT did, f_file_path, status FROM filter")
            return cur.fetchall()

    # did,file_path,status
    def getFilesFilteredDetails(self):
        with open_db_connection(self.path) as cur:
            cur.execute("SELECT did, f_file_path, status, started, finished, exit_code, out, err  FROM filter")
            data = cur.fetchall()
            desc = list(map(lambda x: str(x[0]), cur.description))
            return (desc,data)

    ## Requires tubles of (did,file_path,status)
    def addFileFilteredAll(self, valueList):
        with open_db_connection(self.path) as cur:
            cur.executemany("INSERT INTO filter (did,f_file_path,status) VALUES (?,?,?)",valueList)

    def updateFileFilter(self,update):
        with open_db_connection(self.path) as cur:
            cur.executemany(
                "UPDATE filter SET status=?, started=?,f_file_path=?,finished=?,exit_code=?,out=?,err=? WHERE did = ?",
            update)

####################
## Correlation
    # corr_id, did_a, did_b
    def getCorrelationIds(self):
        with open_db_connection(self.path) as cur:
            cur.execute("SELECT corr_id,did_a,did_b FROM correlation")
            return cur.fetchall()

    # corr_id, did_a, did_b
    def addCorrelationInitAll(self, values):
        with open_db_connection(self.path) as cur:
            cur.executemany("INSERT INTO correlation (corr_id,did_a, did_b,status,run_count) VALUES (?,?,?,'init',0)",values)

    def getCorrelations(self, limit=-1):
        with open_db_connection(self.path) as cur:
            if limit > 0:
                cur.execute("SELECT corr_id,did_a, did_b,status,out from correlation LIMIT %s " % (limit,))
            else:
                cur.execute("SELECT corr_id,did_a, did_b,status,out from correlation")
            return cur.fetchall()


    def getCorrelationRegion(self, start_id, end_id_excl):
        with open_db_connection(self.path) as cur:
            cur.execute("""
            SELECT corr_id,did_a, did_b,status,out
            FROM correlation
            WHERE corr_id >= ?
            AND corr_id < ?
            ORDER BY corr_id
            """,(start_id,end_id_excl))
            return cur.fetchall()

    def getCorrelationOfStatus(self, status,limit=-1):
        with open_db_connection(self.path) as cur:
            if limit > 0:
                cur.execute("""
                SELECT corr_id,did_a, did_b,status,out
                FROM correlation
                WHERE status = ?
                ORDER BY corr_id
                LIMIT %s
                """ % limit,(status,))
            else:
                cur.execute("""
                SELECT corr_id,did_a, did_b,status,out
                FROM correlation
                WHERE status = ?
                ORDER BY corr_id
                """,(status,))

            return cur.fetchall()


    def getCorrelationsDetails(self, id):
        with open_db_connection(self.path) as cur:
            cur.execute("""
            SELECT corr_id,did_a, did_b,status AS STATUS,started AS STARTED,
            finished AS FINISHED,exit_code AS EXIT_CODE,out AS OUTPUT,err AS ERROR
            FROM correlation
            WHERE corr_id = ?
            """,(int(id),))
            data = cur.fetchall()
            desc = list(map(lambda x: str(x[0]), cur.description))
            return (desc,data)

    def updateCorrelationDetails(self, update):
        with open_db_connection(self.path) as cur:
            query = """
            UPDATE correlation
            SET status=?,started=?, finished=?,exit_code=?, out=?,err=?
            WHERE corr_id = ? """
            cur.executemany(query,update)


    def getCorrelationCount(self):
        with open_db_connection(self.path) as cur:
            cur.execute("SELECT count(*) from correlation")
            (cnt,) = cur.fetchone()
            return cnt


####################
## Annotation
    def getFilesAllAnnotated(self):
        with open_db_connection(self.path) as cur:
            query = """
            SELECT e.did AS DATA_FILE_ID,e.external_id AS EXTERNAL_ID,
        group_concat(k.key || '=' || d.value ) AS ANNOTATIONS, e.data_file AS DATA_FILE_PATH
        FROM data_file e, data_key k, data_annotation d
        WHERE e.did = d.did
        AND d.kid = k.kid
        GROUP BY e.did,e.external_id,e.data_file
            """
            cur.execute(query)
            data = cur.fetchall()
            desc = list(map(lambda x: str(x[0]), cur.description))
            return (desc,data)

    # def getFilesCorrelations(self):
    #     with open_db_connection(self.path) as cur:
    #         query = """
    #             SELECT c.corr_id as CORRELATION_ID, f1.f_file_path AS FILE_A,
    #             f2.f_file_path FILE_B,c.status AS STATUS,c.out AS OUTPUT
    #             FROM correlation c, filter f1, filter f2
    #             WHERE c.did_a = f1.did
    #             AND c.did_b = f2.did
    #             """
    #         cur.execute(query)
    #         data = cur.fetchall()
    #         desc = list(map(lambda x: str(x[0]), cur.description))
    #         return (desc,data)

