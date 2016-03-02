from numpy.f2py.rules import sepdict
__author__ = 'mh719'


from exec_util import execCmd
import time
import os
import os.path
import inspect
import chipqc_db

def getHelpInfo():
    return "Enrichment"

def addArguments(parser):
    parser.add_argument('-l','--list-jobs',dest='l_jobs',help="Print jobs",action='store_true')
    parser.add_argument('-L','--list-jobs-details',dest='l_details',help="Print job details",action='store_true')
    parser.add_argument('-o','--out-dir',type=str,dest='out_dir',default='%s/enrichment'%os.getcwd(),help="Output directory of the enrichment results. [default:{0}]".format('%s/enrichment'%os.getcwd()))
    parser.add_argument('-f','--force',dest='force',help="Force recalculation of values",action='store_true')
    parser.add_argument('-j','--job-id',type=int,dest='job_id',help="Enrichment id to process - default: all unprocessed enrichment jobs are run")
    parser.add_argument('-n','--n-jobs',type=int,dest='limit',help="Run n number of jobs - default: no limitations")
    parser.add_argument('-A','--with-annotation',type=str,dest='annot_col',default="INPUT",help="Provide external INPUT id for analysis. [default: INPUT]")
    parser.add_argument('-R','--with-read-count',type=str,dest='annot_cnt',default="READ_COUNT",help="Provide external read counts for analysis. [default: READ_COUNT]")

def getScriptdir():
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

def executeCmd(cmd,storeFunction):
    resList = list()
    resList.append(time.time())
    print (cmd)
#    res = [0,"TEST",""]
    res = execCmd(cmd)
    resList.append(time.time())
    resList.extend(res)
    return storeFunction(resList)

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

    db.updateEnrichment(((status,start,end,exitVal,out,err,id),))
    return id

def _createCommandIdx(db, r_file, cntKey,  jobs):
    data = _loadData(db, cntKey, jobs)
    cmdTemplate = "Rscript {0} --ip-id {1} --ip-mean-file {2} --input-id {3} --input-mean-file {4} --ip-count {5} --input-count {6} --out {7} "
    jobCmds = { x[0] : cmdTemplate.format(r_file, x[1], x[3], x[2], x[4], x[5], x[6], x[7]) for x in data}
    return jobCmds

def _loadData(db, cntKey, jobs):
    id_2_ext = {r[0]:r[1] for r in db.getFiles()}
    id_2_covfile = {r[1]:r[2] for r in db.getCoverage()}
    id_2_count = {r[0]:r[3] for r in db.getAnnotationsByKey(cntKey)}
#    enr = db.getEnrichment()
    enr = jobs

    tmp = list()
    for r in enr:
        id_cnt = 0
        in_cnt = 0
        if r[1] in id_2_count.keys():
            id_cnt = id_2_count[r[1]]
        if r[2] in id_2_count.keys():
            in_cnt = id_2_count[r[2]]

        d = [r[0]]
        d += [id_2_ext[r[1]], id_2_ext[r[2]], id_2_covfile[r[1]], id_2_covfile[r[2]] ]
        d += [id_cnt, in_cnt]
        d += r[3:]
        tmp.append(d)
    return tmp

def printStatus(db, cntKey, jobs, details=False):
    data = _loadData(db, cntKey, jobs)
    descrSum = ["JOB_ID","EXTERNAL_ID","EXTERNAL_ID_INPUT","IP_MEAN","INPUT_MEAN","IP_READ_COUNT","INPUT_READ_COUNT","OUTPUT_DIR","STATUS"]
    descrSum += ["started","finished","exit_code","out","err"]
    descrSum += ["p","q","divergence","z_score","percent_genome_enriched", "input_scaling_factor", "differential_percentage_enrichment"]

    if details :
        _print(descrSum,data)
    else:
        _print(descrSum[0:9], [ r[0:9] for r in data])

def updateDatabase(db, col, out_path):
    ext_to_id = { str(r[1]):r[0] for r in db.getFiles() } ## File IDX
    did_to_ext = { r[1]:r[0] for r in ext_to_id.iteritems() } ## File IDX
    annotated_idx = {r[0]:r for r in db.getAnnotationsByKey(col)} ## did, ext_id, key, value
    ip_to_input = {}
    for r in annotated_idx.values():
        input_ext_id = r[3]
        if input_ext_id not in ext_to_id.keys():
            raise Exception("Annotation {0} of external input id {1} for entry {2} does not exist in DB!!!".format(col,input_ext_id,r[1]))
        input_did = ext_to_id[input_ext_id]
        ip_to_input[r[0]] = input_did

    covdb = [ [row[1],row[2]] for row in db.getCoverage() if row[3] == "done" ] # find finished jobs
    cov_to_file = { r[0]:r[1] for r in covdb }

    enr = db.getEnrichment()
    enr_did = set([r[1] for r in enr])
    print("Found jobs: {0} ...".format(len(enr)))

    max_id = 0
    if len(enr) > 0:
        max_id = max([r[0] for r in enr])

    insert_lst = list()

    for (ip_id,input_did) in ip_to_input.iteritems():
        if  (ip_id in cov_to_file.keys()  ## finished processing mean for IP
             and input_did in cov_to_file.keys()  ## AND input file
             and ip_id not in enr_did): ## not registered yet
            max_id = max_id + 1

            ip_file = cov_to_file[ip_id]
            in_file = cov_to_file[input_did]
            res_dir = "{0}/{1}".format(out_path,did_to_ext[ip_id])
            insert_lst.append([max_id,ip_id, input_did, res_dir,"init"])

    print("Add new jobs: {0} ...".format(len(insert_lst)))
    db.addEnrichment(insert_lst)

def analyseEnrichment(args):
    db_file=args.db_file
    force = args.force
    out_dir=args.out_dir
    col = args.annot_col
    countAnnot = args.annot_cnt
    r_file = getRFile()
    db = chipqc_db.ChipQcDbSqlite(path=db_file)

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    ## Find all jobs (based on annotation)
    updateDatabase(db, col, out_dir)

    allJobs = db.getEnrichment()
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
        printStatus(db, countAnnot, jobs, details=False)
        return
    elif args.l_details:
        printStatus(db, countAnnot, jobs, details=True)
        return

    jobs = [r for r in jobs if force or r[4] == 'init'] ## status is init

    print "Processing %s jobs ..." % (len(jobs))

    ## Build commands
    cmdIdx = _createCommandIdx(db, r_file, countAnnot, jobs)

    ## Execute
    reslist = map(lambda id: executeCmd(cmdIdx[id], lambda x: _storeValue(db,id,x)),cmdIdx.keys())


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
    analyseEnrichment(args)
    return 0