_author__ = 'mh719'


import sqlite3 as lite
import os.path

def getHelpInfo():
    return "list loaded information"

def addArguments(parser):
    parser.add_argument('-l','--loaded-files',dest='l_file',help="Print loaded files",action='store_true')
    parser.add_argument('-L','--loaded-files-annotated',dest='la_file',help="Print loaded files with annotations",action='store_true')
    parser.add_argument('-f','--filtered-files',dest='f_file',help="Print filtered files status",action='store_true')
    parser.add_argument('-c','--correlation',dest='c_list',help="Print Correlation job list with file names, status and output",action='store_true')
    parser.add_argument('-C','--correlation-by-sample-id',dest='C_list',help="Print Correlation job list with Sample Ids, status and output",action='store_true')
    parser.add_argument('-p','--details',type=int,dest='detail_id',help="Detailed information about job id")


def runQuery(args,query,postQuery):
    db_file=args.db_file
    if not os.path.isfile(db_file):
        raise IOError("Database file '%s' does not exist!!! Load data first." % (db_file,))

    with lite.connect(db_file,isolation_level=None) as con:
        cur = con.cursor()
        query(cur)
        postQuery(cur)
        con.commit()

def _filterIdQuery(cur):
    query = "SELECT f.did AS DATA_FILE_ID, f.status as STATUS,f.f_file_path AS FILTERED_FILE_PATH FROM filter f "
    cur.execute(query)

def _loadedIdQuery(cur):
    query = "SELECT e.did AS DATA_FILE_ID,e.external_id AS EXTERNAL_ID, e.data_file AS DATA_FILE_PATH FROM data_file e "
    cur.execute(query)

def _loadedAnnotIdQuery(cur):
    query = """
    SELECT e.did AS DATA_FILE_ID,e.external_id AS EXTERNAL_ID,
    group_concat(d.key || '=' || d.value ) AS ANNOTATIONS, e.data_file AS DATA_FILE_PATH
    FROM data_file e, data_annotation d
    WHERE e.did = d.did
    GROUP BY e.did,e.external_id,e.data_file
    """
    cur.execute(query)

def _correlationQuery(cur):
    query = """
    SELECT c.corr_id as CORRELATION_ID, f1.f_file_path AS FILE_A, f2.f_file_path FILE_B,c.status AS STATUS,c.out AS OUTPUT
    FROM correlation c, filter f1, filter f2
    WHERE c.did_a = f1.did
    AND c.did_b = f2.did
    """
    cur.execute(query)

def _correlationSampleQuery(cur):
    query = """
    SELECT c.corr_id as CORRELATION_ID, f1.external_id EXTERNAL_ID_A, f2.external_id EXTERNAL_ID_B,
           c.status AS STATUS, c.out AS OUTPUT
    FROM correlation c, data_file f1, data_file f2
    WHERE c.did_a = f1.did
    AND c.did_b = f2.did
    """
    cur.execute(query)

def _correlationDetailsQuery(id,cur):
    if None is not None:
        cur.execute("""
         SELECT f.c,c.corr_id
         FROM  correlation c
         JOIN  data_file d1 ON c.did_a = d1.did
         JOIN data_file d2 ON c.did_b = d2.did
         JOIN foo f ON d1.external_id = f.a AND d2.external_id = f.b
         """)
        res = cur.fetchall()
        l = len(res)
        for i in xrange(0, len(res), 1000):
            e = (i+1000)
            print i,e
#            if i == 0:
#                continue
            subList = res[i:(min(l,e))]
            cur.executemany("UPDATE correlation SET out = ? WHERE corr_id = ?",subList)
#        for row in res:
#            cur.execute("UPDATE correlation SET out = ? WHERE corr_id = ?",res[i-1])

    query = """
    SELECT c.corr_id as CORRELATION_ID, f1.external_id AS EXTERNAL_ID_A, f2.external_id EXTERNAL_ID_B,
           c.status AS STATUS, c.started, c.finished, c.exit_code, c.out AS OUTPUT, c.err AS ERROR
    FROM correlation c, data_file f1, data_file f2
    WHERE c.did_a = f1.did
    AND c.did_b = f2.did
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

def loadedAnnotIds(args):
    return runQuery(args=args,query=_loadedAnnotIdQuery,postQuery=_print)

def correlationIds(args):
    return runQuery(args=args,query=_correlationQuery,postQuery=_print)

def correlationSampleIds(args):
    return runQuery(args=args,query=_correlationSampleQuery,postQuery=_print)

def correlationDetails(args,id):
    return runQuery(args=args,query=(lambda x:_correlationDetailsQuery(id,x)),postQuery=_print)

def run(parser,args):
    if args.f_file:
        filterIds(args)
    elif args.l_file:
        loadedIds(args)
    elif args.la_file:
        loadedAnnotIds(args)
    elif args.c_list:
        correlationIds(args)
    elif args.C_list:
        correlationSampleIds(args)
    elif 'detail_id' in args and args.detail_id != None:
        correlationDetails(args,args.detail_id)
    else:
        print "One option required!!!"
        parser.print_help()
        return 1
    return 0