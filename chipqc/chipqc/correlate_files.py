__author__ = 'mh719'


import sqlite3 as lite
import os
import time
import sys
from exec_util import execCmd
import chipqc_db

def getHelpInfo():
    return "Calculate Pearson correlation"

def addArguments(parser):
    parser.add_argument('-w','--wiggle-tool',type=str,dest='wig_tool',default="wiggletools",help="Set path to specific wiggle tool to use")
    parser.add_argument('-f','--force-all',dest='force',help="Force recalculation of values",action='store_true')
    parser.add_argument('-c','--correlation-id',type=int,dest='correlation_id',help="Correlation id to process - default: all correlations with missing values are run")
    parser.add_argument('-l','--limit',type=int,dest='limit',help="Limit numbers of jobs - default: no limitations")

def _loadJobList(db,force=False,limit=-1):
    filter_dict = { id:file for (id,file, stat ) in db.getFilesFiltered()}
    if force:
        res = db.getCorrelations(limit=limit)
    else:
        res = db.getCorrelationOfStatus('init',limit=limit)
    f_list = [ (id,filter_dict[a],filter_dict[b])  for (id,a,b,stat,out) in res ]
    return f_list

def _loadSelectedJobs(db,force=False,correlations=list(),limit=1):
    retList = list()
    filter_dict = { id:file for (id,file, stat ) in db.getFilesFiltered()}

    if limit < 0:
        limit = 1

    for id in correlations:
        res = db.getCorrelationRegion(start_id = int(id), end_id_excl=(int(id)+limit) )
        f_list = [ (id,filter_dict[a],filter_dict[b])  for (id,a,b,stat,out) in res if (force or stat == 'init') ]
        retList.extend(f_list)

    return retList

def executeCmd(cmd,storeFunction):
    res=[-1,"TEST",cmd]
    resList = list()
    resList.append(time.time())
    print (cmd)
    res = execCmd(cmd)
    resList.append(time.time())
    resList.extend(res)
    return storeFunction(resList)

def _processJobs(wigTool,jobList,storeFunction,execFunction):
    cmdTemplate = "{0} pearson {1} {2}"
    jobCmds = {x[0]: cmdTemplate.format(wigTool,x[1],x[2]) for x in jobList}
    reslist = map(lambda id: execFunction(jobCmds[id],lambda x: storeFunction(id,x)),jobCmds.keys())
    return reslist

def _storeValue(db,id,execOut):
    print "%s finished " % id
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

  #   query = """
  #   UPDATE correlation
  #   SET status=?,started=?, finished=?,exit_code=?, out=?,err=?
  #   WHERE corr_id = ?
  # """
    db.getCorrelationsDetails(((status,start,end,exitVal,out,err,id),))
#    con.execute(query,(status,start,end,exitVal,out,err,id))
    return id

def correlate(args):
    force = args.force
    tool=args.wig_tool
    db_file=args.db_file
    db = chipqc_db.ChipQcDbSqlite(path=db_file)

    limit=-1
    if 'limit' in args and args.limit != None:
        limit = args.limit

## Find required Correlation to run
    corrList= list()
    if 'correlation_id' in args and args.correlation_id != None:
        corrList.append(args.correlation_id)
        corrList = _loadSelectedJobs(db,force=force,correlations=corrList, limit=limit)
    else:
        corrList = _loadJobList(db,force=force,limit=limit)

    print "Processing %s jobs ..." % (len(corrList))
    reslist = _processJobs(tool,corrList,lambda id,y: _storeValue(db,id,y),executeCmd)

def run(parser,args):
    correlate(args)
    return 0