from numpy.f2py.rules import sepdict
__author__ = 'mh719'


from exec_util import execCmd
import time
import os
import os.path
import inspect
import chipqc_db

def getHelpInfo():
    return "Snapshot"

def addArguments(parser):
    parser.add_argument('-l','--list-jobs',dest='l_jobs',help="Print jobs",action='store_true')
    parser.add_argument('-o','--out-dir',type=str,dest='out_dir',default='%s/snapshot'%os.getcwd(),help="Output directory of the snapshots. [default:{0}]".format('%s/snapshot'%os.getcwd()))
    parser.add_argument('-j','--job-id',type=int,dest='job_id',help="Id to process - default: all jobs are run")
    parser.add_argument('-n','--n-jobs',type=int,dest='limit',help="Run n number of jobs - default: no limitations")
    parser.add_argument('-r','--regions',type=str,dest='bed_file', required=True,help="Provide BED file with regions (optional ID in 4 column)")
    parser.add_argument('-37','--grch37',dest='rel_37',help="Use GRCh37 release instead of GRCh38",action='store_true')

def getScriptdir():
    return os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

def getRFile():
    cdir=getScriptdir() # script directory
    r_file="%s/snapshot.R" % (cdir,)
    return r_file

def executeCmd(cmd,storeFunction):
    resList = list()
    resList.append(time.time())
    res = execCmd(cmd)
    resList.append(time.time())
    resList.extend(res)
    return storeFunction(resList)

def _storeValue(db,id,execOut):
    start=execOut[0]
    end=execOut[1]
    exitVal = execOut[2]
    out = execOut[3]
    err = execOut[4]

    if out is not None:
        out = out.strip()
    if err is not None:
        err = err.strip()

    if exitVal != 0:
        print "Issue with Job {0}: {1} {2}".format(id,out,err)
    else:
        print "Job %s finished " % id
    return id

def _createCommandIdx(db, r_file, out_dir, bed_file, jobs, grch37=False):
    data = jobs
    grOption=""
    if grch37:
        grOption="--grch37"

    cmdTemplate = "Rscript {0} --out-dir {1} --external-id {2} --file-bw {3} --regions-bed {4} {5}"
    jobCmds = { x[0] : cmdTemplate.format(r_file, out_dir, x[1], x[2], bed_file, grOption) for x in data}
    return jobCmds

def printStatus(db, jobs):
    data = jobs
    descrSum = ["DATA_FILE_ID","EXTERNAL_ID","DATA_FILE_PATH"]
    print "\t".join(descrSum)
    for row in data:
        lst = [str(x) for x in row]
        print "\t".join(lst)

def runScreenshots(args):
    db_file=args.db_file
    out_dir=args.out_dir
    r_file = getRFile()
    bed_file=os.path.abspath(args.bed_file)
    db = chipqc_db.ChipQcDbSqlite(path=db_file)

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    allJobs = db.getFiles()
    jobs = allJobs

    limit=-1
    if 'limit' in args and args.limit != None:
        limit = args.limit

    if 'job_id' in args and args.job_id != None:
        max_id = args.job_id + 1
        if limit > 0:
            max_id = args.job_id + limit
        jobs = [ r for r in jobs if r[0] >= args.job_id and r[0] < max_id]

## Print INFO
    if args.l_jobs:
        printStatus(db, jobs)
        return

    print "Processing %s jobs ..." % (len(jobs))

    ## Build commands
    cmdIdx = _createCommandIdx(db, r_file, out_dir, bed_file, jobs, grch37=args.rel_37)

    ## Execute
    reslist = map(lambda id: executeCmd(cmdIdx[id], lambda x: _storeValue(db,id,x)),cmdIdx.keys())
    return None

def run(parser,args):
    runScreenshots(args)
    return 0