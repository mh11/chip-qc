from numpy.f2py.rules import sepdict
__author__ = 'mh719'


from exec_util import execCmd
import os
import os.path
import inspect
import chipqc_db

def getHelpInfo():
    return "Enrichment"

def addArguments(parser):
    parser.add_argument('-l','--list-files',dest='l_file',help="Print annotated files",action='store_true')
    parser.add_argument('-L','--list-files-annotated',dest='la_file',help="Print processed files with details",action='store_true')
    parser.add_argument('-o','--out-dir',type=str,dest='out_dir',default='%s/enrichment'%os.getcwd(),help="Output directory of the enrichment results. [default:{0}]".format('%s/enrichment'%os.getcwd()))
    parser.add_argument('-f','--force',dest='force',help="Force recalculation of values",action='store_true')
    parser.add_argument('-i','--id',type=int,dest='id',help="Enrichment id to process - default: all unprocessed enrichment jobs are run")
    parser.add_argument('-x','--maximum',type=int,dest='limit',help="Limit numbers of jobs - default: no limitations")

def getScriptdir():
#    print inspect.getfile(inspect.currentframe()) # script filename (usually with path)
    return os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

def getRFile():
    cdir=getScriptdir() # script directory
    r_file="%s/enrichment/enrichment.R" % (cdir,)
    return r_file


def _print(header,data):
    print "\t".join(header)
    for row in data:
        lst = [str(x) for x in row]
        print "\t".join(lst)

def _loadData(db,col):
    (descrSum, dataSum) = db.getEnrichmentAnnotated(col=col)
    currStatus = { id:status for (id,status) in db.getEnrichmentStatus() }

    idx = { str(row[1]):str(row[2])  for row in db.getFiles()}

    tmp = list()
    for row in dataSum:
        id = row[0]
        inid = row[2]
        if inid not in idx.keys():
            raise Exception("Input ID {0} not found in DB!!!".format(inid) )
        status = "new"

        if id in currStatus.keys():
            status = str(currStatus[id])

        row += (idx[inid],status)
        tmp.append(row )

    descrSum += ("INPUT_DATA_FILE_PATH","STATUS")
    dataSum = tmp
    return (descrSum,dataSum)

def printStatus(db, col, details=False):
    (descrSum, dataSum) = _loadData(db, col)
    if details :
        _print(descrSum,dataSum)
    else:
        _print(descrSum[0:3], [ r[0:3] for r in dataSum])

def analyseCorrelation(args):
    db_file=args.db_file
    force = args.force
    wig=args.wig_tool
    out_dir=args.out_dir

    r_file = getRFile()
#    print("RFile: %s" % r_file)

    db = chipqc_db.ChipQcDbSqlite(path=db_file)

## Print INFO
    if args.l_file:
        printStatus(db, args.in_col, details=False)
        return;
    elif args.la_file:
        printStatus(db, args.in_col, details=True)
        return;

    limit=-1
    if 'limit' in args and args.limit != None:
        limit = args.limit

    jobList= [ row for row in _loadData(db,args.in_col)[1] if force or row[5] == "new" ]
    if 'id' in args and args.id != None:
        maxid = args.id + 1
        if limit > 0:
            maxid = args.id + limit
        jobList = [ row for row in jobList if row[0] >= args.id and row[0] < maxid]

    print("Found %s job(s) to process ... " % len(jobList))

    print("Calculate mean coverage ... ")
    _calculateMean(db, args.in_col, out_bed_dir, force=force)

    ## TODO implement
    ## Calculate mean value for each BIN (done before
    ## INPUT
    ##   -> calculate mean
    ## IP files
    ##   -> calculate mean
    ##
    ## wiggletools apply_paste mean.txt meanI regions.chr.bed BW

    ## execute Enrichment R script to
    ##  -> Plot + result file
    ##  -> read result file and put into DB


#    cmdTemplate=" Rscript {0} --db {1} --out {2}"

#    cmd=cmdTemplate.format(r_file,db_file,out_dir)
#    res = execCmd(cmd)

#    start=res[0]
#    end=res[1]
#    exitVal = res[2]
#    out = res[3]
#    err = res[4]

#    print (res)
    return None

def run(parser,args):
    analyseCorrelation(args)
    return 0