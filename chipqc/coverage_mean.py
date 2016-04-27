__author__ = 'mh719'


import sqlite3 as lite
import os
import time
import sys
from exec_util import execCmd
import chipqc_db
import inspect

def getHelpInfo():
    return "Calculate Mean coverage across binned regions - run only on columns with selected annotation column present"

def addArguments(parser):
    parser.add_argument('-l','--list-jobs',dest='l_jobs',help="Print jobs",action='store_true')
    parser.add_argument('-L','--list-jobs-details',dest='l_details',help="Print job details",action='store_true')
    parser.add_argument('-o','--out-dir',type=str,dest='out_dir',default='{0}/coverage'.format(os.getcwd()),help="Output directory of the coverage results. [default:{0}]".format('%s/coverage'%os.getcwd()))
    parser.add_argument('-w','--wiggle-tool',type=str,dest='wig_tool',default="wiggletools",help="Set path to specific wiggle tool to use")
    parser.add_argument('-f','--force',dest='force',help="Force recalculation of values",action='store_true')
    parser.add_argument('-j','--job-id',type=int,dest='job_id',help="Job id to process - default: all jobs with missing values are run")
    parser.add_argument('-n','--n-jobs',type=int,dest='limit',help="Run n number of jobs - default: no limitations")
    parser.add_argument('-g','--genome',type=str,dest='genome',default="GRCh38",help="Genome assembly version")
    parser.add_argument('-C','--chromosome-prefix',type=str,dest='chr_exist',default="True",help="Chromosomes use 'chr' prefix (True|False) [default: True]")
    parser.add_argument('-A','--with-annotation-only',type=str,dest='annot_col',default="CALC_MEAN",help="Limit to experiements annotated with key. [default: CALC_MEAN]")

def getScriptdir():
#    print inspect.getfile(inspect.currentframe()) # script filename (usually with path)
    return os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

def getBedFile(genome="GRCh38", prefix=True):
    cdir=getScriptdir() # script directory
    chr=""
    if prefix:
        chr= ".chr"
    bed_file="{0}/enrichment/{1}{2}.bed".format(cdir,genome,chr)
    return bed_file

def executeCmd(cmd,storeFunction):
#    res=[1,2,-1,"TEST",cmd]
    resList = list()
    resList.append(time.time())
    print (cmd)
#    res = [0,"TEST",""]
    res = execCmd(cmd)
    resList.append(time.time())
    resList.extend(res)
    return storeFunction(resList)

def _createCommandIdx(db, wigTool, bedFile, jobList):
#    wiggletools apply_paste mean.txt meanI regions.chr.bed BW
    cmdTemplate = "{0} apply_paste {1} meanI {2} {3}"
    id2bw = {r[0]:r[2] for r in db.getFiles()}
    jobCmds = { x[0] : cmdTemplate.format(wigTool, x[2], bedFile, id2bw[x[1]]) for x in jobList}
    return jobCmds

def _storeValue(db,id,execOut):
    print "Job %s finished " % id
    start=execOut[0]
    end=execOut[1]
    exitVal = execOut[2]
    out = execOut[3]
    err = execOut[4]

    if out is not None:
        out = out.strip()
    if err is not None:
        err = err.strip()

    status='done'
    if exitVal != 0:
        status='error'

    db.updateCoverageDetails(((status,start,end,exitVal,out,err,id),))
    return id

def updateDatabase(db, col, out_path):
    annotated = db.getAnnotationsByKey(col)
    covdb = db.getCoverage()
    covIds = set([ r[1] for r in covdb ])

    ## not seen before
    new_annot = [r for r in annotated if r[0] not in covIds]

    if len(new_annot) > 0:
        print("Create {0} new Coverage jobs ...".format(len(new_annot)))
        maxid = 0
        if len(covdb) > 0:
            maxid = max([r[0] for r in covdb])
        insert_lst = list()
        for row in new_annot:
            maxid = maxid + 1
            extId = row[1]
            ofile = "{0}/{1}.bed".format(out_path,extId)
            insert_lst.append([maxid,row[0],ofile,'init'])
        db.addCoverage(insert_lst)

def printStatus(db, data, details=False):
    extIdx = {r[0]:r[1] for r in db.getFiles()}
    header=["JOB_ID","DATA_FILE_ID", "EXTERNAL_ID","COVERAGE_BED","STATUS"]
    if details:
        header += ["started","finished","exit_code","out","err"]
    print "\t".join(header)
    for row in data:
        d = list()
        d += row[0:2]
        d += [extIdx[row[1]]]
        d += row[2:4]
        if details:
            d += row[4:]
        lst = [str(x) for x in d]
        print "\t".join(lst)

def calculate(args):
    force = args.force
    tool=args.wig_tool
    db_file=args.db_file
    db = chipqc_db.ChipQcDbSqlite(path=db_file)
    col = args.annot_col
    out_dir=args.out_dir

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    ## Find all jobs (based on annotation)
    updateDatabase(db, col, out_dir)

    ## Load all jobs
    allJobs = db.getCoverage()


## Find required Jobs to run

    jobs = allJobs

    limit=-1
    if 'limit' in args and args.limit != None:
        limit = args.limit

    if 'job_id' in args and args.job_id != None:
        max_id = args.job_id + 1
        if limit > 0:
            max_id = args.job_id + limit
        jobs = [ r for r in jobs if r[0] >= args.job_id and r[0] < max_id]

    if args.l_jobs:
        printStatus(db, jobs)
        return;
    if args.l_details:
        printStatus(db, jobs, details=True)
        return;

    jobs = [r for r in jobs if force or r[3] == 'init'] ## status is init

    print "Processing %s jobs ..." % (len(jobs))

    ## Build commands
    cmdIdx = _createCommandIdx(db,tool,getBedFile(genome=args.genome,prefix= args.chr_exist == "True"),jobs)

    ## Execute
    reslist = map(lambda id: executeCmd(cmdIdx[id], lambda x: _storeValue(db,id,x)),cmdIdx.keys())

def run(parser,args):
    calculate(args)
    return 0