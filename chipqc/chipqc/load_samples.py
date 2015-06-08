__author__ = 'mh719'

import csv
import sqlite3 as lite
import os.path

def parseCsv(file):
    exp = list()

    with open(file,"r") as tsvfile:
        tsvreader = csv.reader(tsvfile, delimiter="\t")
        hpos = {}
        for idx,line in enumerate(tsvreader):
            if idx == 0:
                # Header line
                for hidx,key in enumerate(line):
                    hpos[key.upper()] = hidx
#                print tuple(hpos[i] for i in ('EXPERIMENT_ID','SAMPLE_ID','SAMPLE_NAME','EXPERIMENT_TYPE','CELL_TYPE','URI'))
            else:
                entry = (idx,) + tuple(line[hpos[i]] for i in ('EXPERIMENT_ID','SAMPLE_ID','SAMPLE_NAME','EXPERIMENT_TYPE','CELL_TYPE','URI'))
                exp.append(entry)
    return exp

def createExp(cur,data,isNew=True):
    ## DON'T drop automatically
#    cur.execute("DROP TABLE IF EXISTS exp")
    if isNew:
        cur.execute("CREATE TABLE exp(id INT UNIQUE, exp_id TEXT UNIQUE, sample_id TEXT, sample_name TEXT,exp_type TEXT, cell_type TEXT, file TEXT)")
    cur.executemany("INSERT INTO exp VALUES(?, ?, ?, ?, ?, ?, ?)", data)

def countExp(cur):
    cur.execute("SELECT count(*) FROM exp")
    (cnt,) = cur.fetchone()
    return cnt

def createFilter(cur,isNew=True):
    if isNew:
        cur.execute("CREATE TABLE filter(id INT UNIQUE,f_file_path TEXT, status TEXT, started TEXT, finished TEXT, exit_code TEXT,out TEXT,err TEXT)")
    cur.execute("INSERT INTO filter (id,status) SELECT id,'init' FROM exp")

def createCorrelation(cur,isNew=True):
    if isNew:
        cur.execute("CREATE TABLE correlation(corr_id INT,id_a INT, id_b INT, status TEXT, started TEXT, finished TEXT, exit_code TEXT, out TEXT, err TEXT,run_count INT DEFAULT 0) ")

    cur.execute("SELECT id FROM exp ORDER BY id ASC")
    ids = list(row[0] for row in cur.fetchall())
    corr_cnt = 1
    for a in ids:
        corr_pair = tuple()
        for b in ids:
            if a < b: # only one side of the matrix
                corr_pair += ((corr_cnt,a,b),)
                corr_cnt += 1
        cur.executemany("INSERT INTO correlation (corr_id,id_a, id_b,status,run_count) VALUES (?,?,?,'init',0)",corr_pair)

def countCorr(cur):
    cur.execute("SELECT count(*) from correlation")
    (cnt,) = cur.fetchone()
    return cnt

def load(args):
    db_file=args.db_file
    mf_file = args.mf_file

    new_db = (not os.path.isfile(db_file))

    print "Loading Manifest file %s " %(os.path.abspath(mf_file),)
    exp = parseCsv(file=mf_file)
    print("Entries found in file: %s " % len(exp))

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
        createCorrelation(cur,isNew=new_db)

        corrCnt = countCorr(cur)
        print ("Prepared correlations: %s" % corrCnt)

    print("Done")

