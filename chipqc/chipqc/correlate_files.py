__author__ = 'mh719'


import sqlite3 as lite
import os
import time
import sys
from exec_util import execCmd

def _fetchList(con):
    res = con.fetchall()
    return res

def _loadJobList(con,force=False,limit=-1):
    query = """
        SELECT c.corr_id, f1.f_file_path, f2.f_file_path
        FROM correlation c, filter f1, filter f2
        WHERE c.id_a = f1.id
        AND c.id_b = f2.id
        """
    if not force:
        query += " AND c.status = 'init' "

    if limit>=0:
        query += "ORDER BY c.corr_id LIMIT %s" % limit

    con.execute(query)
    return _fetchList(con)

def _loadSelectedJobs(con,force=False,correlations=list(),limit=1):
    retList = list()
    if limit < 0:
        limit = 1

    query = """
        SELECT c.corr_id, f1.f_file_path, f2.f_file_path
        FROM correlation c, filter f1, filter f2
        WHERE c.id_a = f1.id
        AND c.id_b = f2.id
        AND c.corr_id >= %s
        """
    if not force:
        query += " AND c.status = 'init' "

    query += " ORDER BY c.corr_id LIMIT %s" % limit

    for id in correlations:
        con.execute(query % id)
        lst = _fetchList(con)
        retList.extend(lst)
    return retList

def executeCmd(cmd,storeFunction):
    res=[-1,"TEST",cmd]
    resList = list()
    resList.append(time.time())
    res = execCmd(cmd)
    resList.append(time.time())
    resList.extend(res)
    return storeFunction(resList)

def _processJobs(wigTool,jobList,storeFunction,execFunction):
    cmdTemplate = "{0} pearson {1} {2}"
    jobCmds = {x[0]: cmdTemplate.format(wigTool,x[1],x[2]) for x in jobList}
    reslist = map(lambda id: execFunction(jobCmds[id],lambda x: storeFunction(id,x)),jobCmds.keys())
    return reslist

def _storeValue(con,id,execOut):
    print "%s finished " % id
    start=execOut[0]
    end=execOut[1]
    exitVal = execOut[2]
    out = execOut[3]
    err = execOut[4]
    status='done'
    if exitVal != 0:
        status='error'

    query = """
    UPDATE correlation
    SET status=?,started=?, finished=?,exit_code=?, out=?,err=?
    WHERE corr_id = ?
  """
    con.execute(query,(status,start,end,exitVal,out,err,id))
    return id

def correlate(args):
    force = args.force
    tool=args.wig_tool
    db_file=args.db_file
    limit=-1
    if 'limit' in args and args.limit != None:
        limit = args.limit

## Find required Correlation to run
    corrList= list()
    with lite.connect(db_file,timeout=30.0) as con:
        cur = con.cursor()
        if 'correlation_id' in args and args.correlation_id != None:
            corrList.append(args.correlation_id)
            corrList = _loadSelectedJobs(cur,force=force,correlations=corrList, limit=limit)
        else:
            corrList = _loadJobList(cur,force=force,limit=limit)

        print "Processing jobs ..."
## Run correlations
        reslist = _processJobs(tool,corrList,lambda id,y: _storeValue(cur,id,y),executeCmd)


