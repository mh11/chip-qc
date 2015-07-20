__author__ = 'mh719'

import csv
import sqlite3 as lite
import os.path

def parseCsv(file):
    exp = list()

    with open(file,"r") as tsvfile:
        tsvreader = csv.reader(tsvfile, delimiter="\t")
        hpos = {}
        colLen = 0
        for idx,line in enumerate(tsvreader):
            if idx == 0:
                colLen = len(line)
                entry = ()
                # Header line
                for hidx,key in enumerate(line):
                    if(hidx > 1):# Only annotations
                        entry += (key,)
                exp.append(entry)
            else:
                if len(line) != colLen:
                    raise ValueError("Row %s has not matching number of columns: %s instead of %s" % (idx,len(line),colLen))
                exp.append(line)
    return exp

def createExp(cur,data,isNew=True):
    ## DON'T drop automatically
#    cur.execute("DROP TABLE IF EXISTS exp")
    currMaxId = 0
    if isNew:
#        cur.execute("CREATE TABLE exp(id INT UNIQUE, exp_id TEXT UNIQUE, sample_id TEXT, sample_name TEXT,exp_type TEXT, cell_type TEXT, file TEXT)")
        cur.execute("CREATE TABLE data_file(did INT UNIQUE, external_id TEXT UNIQUE, data_file TEXT UNIQUE)")
        cur.execute("CREATE TABLE data_annotation(did INT, key TEXT,value TEXT)")
    else:
        cur.execute("SELECT MAX(did) FROM data_file")
        currMaxId = cur.fetchone()[0]
        if currMaxId == None:
            currMaxId = 0

    annot_desc = []
    dQuery = "INSERT INTO data_file (did,external_id,data_file) VALUES(?,?,?)"
    aQuery = "INSERT INTO data_annotation(did,key,value) VALUES (?,?,?)"

    for idx,row in enumerate(data):
        if idx == 0:
            annot_desc = row
        else:
            qid = currMaxId + idx
            qdata = [qid,row[0],row[1]]
            cur.execute(dQuery, qdata)
            for aidx,ae in enumerate(annot_desc):

                qdata = [qid,ae,row[2+aidx]]
                cur.execute(aQuery,qdata)

def countExp(cur):
    cur.execute("SELECT count(*) FROM data_file")
    (cnt,) = cur.fetchone()
    return cnt

def createFilter(cur,isNew=True):
    if isNew:
        cur.execute("CREATE TABLE filter(did INT UNIQUE,f_file_path TEXT, status TEXT, started TEXT, finished TEXT, exit_code TEXT,out TEXT,err TEXT)")
    cur.execute("INSERT INTO filter (did,f_file_path,status) SELECT did,data_file,'init' FROM data_file d WHERE d.did not in (SELECT did from filter)")

def createCorrelation(cur,isNew=True):
    currentIdx = set()
    maxId = 0
    if isNew:
        cur.execute("CREATE TABLE correlation(corr_id INT,did_a INT, did_b INT, status TEXT, started TEXT, finished TEXT, exit_code TEXT, out TEXT, err TEXT,run_count INT DEFAULT 0) ")
    else:
        cur.execute("SELECT corr_id,did_a,did_b FROM correlation")
        for x in cur.fetchall():
            currentIdx.add(str(x[1])+"_"+str(x[2]))
            maxId = max(x[0],maxId)

    cur.execute("SELECT did FROM data_file ORDER BY did ASC")
    ids = list(row[0] for row in cur.fetchall())
    corr_cnt = 1
    for a in ids:
        corr_pair = tuple()
        for b in ids:
            if a < b: # only one side of the matrix
                if (str(a) + "_" +str(b)) not in currentIdx:
                    corr_pair += ((maxId+corr_cnt,a,b),)
                    corr_cnt += 1
        cur.executemany("INSERT INTO correlation (corr_id,did_a, did_b,status,run_count) VALUES (?,?,?,'init',0)",corr_pair)
    return corr_cnt

def countCorr(cur):
    cur.execute("SELECT count(*) from correlation")
    (cnt,) = cur.fetchone()
    return cnt

def load(args):
    db_file=args.db_file
    dataFile = args.fl_file

    new_db = (not os.path.isfile(db_file))

    print "Loading file %s " %(os.path.abspath(dataFile),)
    exp = parseCsv(file=dataFile)
    print("Entries found in file: %s " % (len(exp)-1))

## Store in DB
    with lite.connect(db_file) as con:
        cur = con.cursor()

        ## Create EXP table
        print("Loading data into DB ... ")
        createExp(cur,exp,isNew=new_db)
        con.commit()
        dbExpCnt = countExp(cur)

        print("Entries found in DB: %s " % dbExpCnt)

        ## Filter
        createFilter(cur,isNew=new_db)

       ## Create correlation table
        newCnt = createCorrelation(cur,isNew=new_db)

        corrCnt = countCorr(cur)
        print ("Prepared correlations: %s new from %s" % (newCnt,corrCnt))

    print("Done")

