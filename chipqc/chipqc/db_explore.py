_author__ = 'mh719'


import sqlite3 as lite
import os.path



def runQuery(args,query,postQuery):
    db_file=args.db_file
    if not os.path.isfile(db_file):
        raise IOError("Database file '%s' does not exist!!! Load data first." % (db_file,))

    with lite.connect(db_file) as con:
        cur = con.cursor()
        query(cur)
        postQuery(cur)

def _filterIdQuery(cur):
    query = "SELECT f.id AS FILTER_FILE_ID,f.f_file_path AS FILE_PATH, f.status as STATUS FROM filter f "
    cur.execute(query)

def _loadedIdQuery(cur):
    query = "SELECT e.id AS LOAD_FILE_ID,e.sample_id AS SAMPLE_ID, e.file AS FILE_PATH FROM exp e "
    cur.execute(query)

def _correlationQuery(cur):
    query = """
    SELECT c.corr_id as CORRELATION_ID, f1.f_file_path AS FILE_A, f2.f_file_path FILE_B,c.status AS STATUS,c.out AS OUTPUT
    FROM correlation c, filter f1, filter f2
    WHERE c.id_a = f1.id
    AND c.id_b = f2.id
    """
    cur.execute(query)

def _correlationSampleQuery(cur):
    query = """
    SELECT c.corr_id as CORRELATION_ID, f1.exp_id EXPERIMENT_A, f2.exp_id EXPERIMENT_B,c.status AS STATUS,c.out AS OUTPUT
    FROM correlation c, exp f1, exp f2
    WHERE c.id_a = f1.id
    AND c.id_b = f2.id
    """
    cur.execute(query)

def _correlationDetailsQuery(id,cur):
    query = """
    SELECT c.corr_id as CORRELATION_ID, f1.exp_id AS EXPERIMENT_A, f2.exp_id EXPERIMENT_B,c.status AS STATUS,c.out AS OUTPUT, c.started, c.finished, c.exit_code, c.err AS ERROR
    FROM correlation c, exp f1, exp f2
    WHERE c.id_a = f1.id
    AND c.id_b = f2.id
    AND c.corr_id = %s
    """
    cur.execute(query % id)

def _print(cur):
    res = cur.fetchall()
    desc=cur.description
    names = list(map(lambda x: str(x[0]), desc))
    print "\t".join(names)
    for row in res:
        lst = list([str(x) for x in row])
#        print lst
        print "\t".join(lst)


def filterIds(args):
    return runQuery(args=args,query=_filterIdQuery,postQuery=_print)

def loadedIds(args):
    return runQuery(args=args,query=_loadedIdQuery,postQuery=_print)

def correlationIds(args):
    return runQuery(args=args,query=_correlationQuery,postQuery=_print)

def correlationSampleIds(args):
    return runQuery(args=args,query=_correlationSampleQuery,postQuery=_print)

def correlationDetails(args,id):
    return runQuery(args=args,query=(lambda x:_correlationDetailsQuery(id,x)),postQuery=_print)

