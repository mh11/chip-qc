__author__ = 'mh719'

import csv
import sqlite3 as lite
import os.path
import chipqc_db

def getHelpInfo():
    return 'load sample information'

def addArguments(parser):
    parser.add_argument('-f','--file-list',type=str,dest='fl_file',required=True,help="A tab separated file (incl. header) with three (or more) columns: <id> <file> <annotation> [<annotations>]")

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

def createExp(db,data):

    currMaxId = db.getMaxDataId()
    extIds = set([ext for did,ext,f in db.getFiles()])
    annot_desc = []
    file_tupels = tuple()
    annot_tupels = tuple()

    for idx,row in enumerate(data):
        if idx == 0:
            annot_desc = row
        elif row[0] not in extIds:
            print (row[0])
            qid = currMaxId + idx
            file_tupels += ((qid,row[0],row[1]),)
            for aidx,ae in enumerate(annot_desc):
                annot_tupels += ((qid,ae,row[2+aidx]),)
    ## Store
    db.addDataFileAll(file_tupels)
    db.addAnnotationAll(annot_tupels)

def countExp(db):
    return db.getFileCount()

def createFilter(db):
    filtCurr = set([did for did,f,s in db.getFilesFiltered()])
    filtered = [ (did,f,'init') for (did,ext,f) in db.getFiles() if did not in filtCurr]
    db.addFileFilteredAll(filtered)

def createCorrelation(db):
    corrRes = db.getCorrelationIds()
    currentIdx = set([ (str(a)+"_"+str(b)) for cid,a,b in corrRes])
    maxId = 0
    if len(corrRes) > 0:
        maxId = max([cid for cid,a,b, in corrRes])

    ids = sorted(list(did for did,ext,f in db.getFiles()))
    corr_cnt = 0
    for a in ids:
        corr_pair = tuple()
        for b in ids:
            if a < b: # only one side of the matrix
                if (str(a) + "_" +str(b)) not in currentIdx:
                    corr_cnt += 1
                    nid = maxId+corr_cnt
                    corr_pair += ((nid,a,b),)
        db.addCorrelationInitAll(corr_pair)
#        cur.executemany("INSERT INTO correlation (corr_id,did_a, did_b,status,run_count) VALUES (?,?,?,'init',0)",corr_pair)
    return corr_cnt

def load(args):
    db_file=args.db_file
    db = chipqc_db.ChipQcDbSqlite(path=db_file)

    dataFile = args.fl_file

    # new_db = (not os.path.isfile(db_file))

    print "Loading file %s " %(os.path.abspath(dataFile),)
    exp = parseCsv(file=dataFile)
    print("Entries found in file: %s " % (len(exp)-1))

## Store in DB
    print("Loading data into DB ... ")
    createExp(db,exp)
    dbExpCnt = countExp(db)

    print("Entries found in DB: %s " % dbExpCnt)

       ## Filter
    createFilter(db)

       # ## Create correlation table
    newCnt = createCorrelation(db)

    corrCnt = db.getCorrelationCount()
    print ("Prepared correlations: %s new from %s" % (newCnt,corrCnt))

    print("Done")

def run(parser,args):
    args.fl_file = os.path.abspath(args.fl_file)
    load(args)
