_author__ = 'mh719'


import sqlite3 as lite
import os.path
import chipqc_db

def getHelpInfo():
    return "list loaded information"

def addArguments(parser):
    parser.add_argument('-l','--loaded-files',dest='l_file',help="Print loaded files",action='store_true')
    parser.add_argument('-L','--loaded-files-annotated',dest='la_file',help="Print loaded files with annotations",action='store_true')
    parser.add_argument('-f','--filtered-files',dest='f_file',help="Print filtered files status",action='store_true')
    parser.add_argument('-F','--filtered-files-details',dest='f_file_detail',help="Print filtered files detailed status",action='store_true')
    parser.add_argument('-c','--correlation',dest='c_list',help="Print Correlation job list with file names, status and output",action='store_true')
    parser.add_argument('-C','--correlation-by-sample-id',dest='C_list',help="Print Correlation job list with Sample Ids, status and output",action='store_true')
    parser.add_argument('-p','--details',type=int,dest='detail_id',help="Detailed information about job id")

## for test filling data
def _patch(id,cur):
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

def _print(header,data):
    print "\t".join(header)
    for row in data:
        lst = [str(x) for x in row]
#        print lst
        print "\t".join(lst)

def filterIds(db):
    data = [[a,c,b] for a,b,c in db.getFilesFiltered()]
    _print(["DATA_FILE_ID","STATUS","FILTERED_FILE_PATH"],data)

def filterDetailsIds(db):
    (desc,data) = db.getFilesFilteredDetails()
    _print(desc,data)

def loadedIds(db):
    _print(["DATA_FILE_ID","EXTERNAL_ID","DATA_FILE_PATH"],
           db.getFiles())

def loadedAnnotIds(db):
    (descr,data) = db.getFilesAllAnnotated()
    _print(descr,data)

def correlationIds(db):
    fileIdx = { a:(a,fp,status) for a,fp,status in db.getFilesFiltered()}
    res = [ [id,fileIdx[a][1],fileIdx[b][1],status,out] for id,a,b,status,out in db.getCorrelations()]
    _print(["CORRELATION_ID","FILE_A","FILE_B","STATUS","OUTPUT"],
           res)

def correlationSampleIds(db):
    fileIdx = { a:(a,fp,status) for a,fp,status in db.getFiles()}
    res = [ [id,fileIdx[a][1],fileIdx[b][1],status,out] for id,a,b,status,out in db.getCorrelations()]
    _print(["CORRELATION_ID","EXTERNAL_ID_A","EXTERNAL_ID_B","STATUS","OUTPUT"],
           res)

def correlationDetails(db,id):
    fileIdx = { a:(a,fp,status) for a,fp,status in db.getFiles()}
    (desc,data) = db.getCorrelationsDetails(id)
    res = [ [cid,fileIdx[a][1],fileIdx[b][1],status,s,f,ec,out,err] for cid,a,b,status,s,f,ec,out,err in data]
    _print(["CORRELATION_ID","EXTERNAL_ID_A","EXTERNAL_ID_B","STATUS","STARTED","FINISHED","EXIT_CODE","OUTPUT","ERROR"],
           res)

def run(parser,args):
    db_file=args.db_file
    db = chipqc_db.ChipQcDbSqlite(path=db_file)
    if args.f_file:
        filterIds(db)
    elif args.f_file_detail:
        filterDetailsIds(db)
    elif args.l_file:
        loadedIds(db)
    elif args.la_file:
        loadedAnnotIds(db)
    elif args.c_list:
        correlationIds(db)
    elif args.C_list:
        correlationSampleIds(db)
    elif 'detail_id' in args and args.detail_id != None:
        correlationDetails(db,args.detail_id)
    else:
        print "One option required!!!"
        parser.print_help()
        return 1
    return 0