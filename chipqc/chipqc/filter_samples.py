__author__ = 'mh719'

import sqlite3 as lite
import os
import time
import sys
from exec_util import execCmd
import chipqc_db

def getHelpInfo():
    return "[OPTIONAL STEP] Filter wiggle files"

def addArguments(parser):
    parser.add_argument('-s','--skip',dest='skip',help="Skip filtering - use original files",action='store_true')
    parser.add_argument('-r','--regulatory-build',type=str,dest='reg_build',help="RegBuild.bb file")
    parser.add_argument('-o','--out-dir',type=str,dest='out_dir',default='wiggle-filtered',help="Output directory of the filtered wiggle file")
    parser.add_argument('-d','--data_id',type=int,dest='data_id',help="File id to process - default all files are processed")
    parser.add_argument('-w','--wiggle-tool',type=str,dest='wig_tool',default="wiggletools",help="Set path to specific wiggle tool to use")
    parser.add_argument('-f','--force-all',dest='force',help="Force refilter",action='store_true')

def skipFilter(db,args):
    filterid = list()
## Store in DB
    file_data = db.getFiles()

    if 'data_id' in args and args.data_id != None:
        filterid.append(int(args.data_id))
    else:
        filterid = [ int(row[0]) for row in file_data]

    print ("Skip %s files ... " % (len(filterid)))
    now=time.time()
    filterupdate = [( "done",now,row[2],now,0,int(row[0]) ) for row in file_data if int(row[0]) in filterid]
    db.updateFileFilter(filterupdate)
    print ("Updated %s filter files. " % (len(filterupdate)))


def filter(db,args):
    ret = 0
    out_dir = args.out_dir
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    filterid = list()
## Store in DB
    file_data = db.getFiles()

    if 'data_id' in args and args.data_id != None:
        filterid.append(int(args.data_id))
    else:
        filterid = [ int(row[0]) for row in file_data]

    currFilterDetails = db.getFilesFilteredDetails()[1]

    for fid in filterid:
#        cur.execute("SELECT did,data_file from data_file WHERE did = ? ",(fid,))
        url = [row[2] for row in file_data if int(row[0]) == fid][0]
#        cur.execute("SELECT exit_code,status from filter WHERE did = ? ",(fid,))
        (curr_code,curr_status) = [(row[5],row[2]) for row in currFilterDetails if int(row[0]) == fid][0]

        ofile=out_dir+"/"+str(fid)+".filtered.wig"

        print("For %s found url %s to store as %s ..." % (fid,url,ofile))

        if curr_code is not None:
            print("Already processed finished with %s" % curr_code)

            if curr_code == "0" or curr_code == 0:
                print ("Already downloaded - done")
                if 'force' in args and args.force:
                    print ("Force rerun of %s " % ofile)
                else:
                    continue

            if os.path.isfile(ofile):
                print ("remove file %s " % ofile)
                os.unlink(ofile) # clean up

#            oerr=out_dir+"/"+exp_id+".filtered.err"
#            olog=out_dir+"/"+exp_id+".filtered.log"
        nowStart=time.time()

        # status=?, started=?,f_file_path=?,finished=?,exit_code=?
        db.updateFileFilter((("started",nowStart,ofile,None,None,None,None,fid),))

        cmd="%s write %s mult %s %s" % (args.wig_tool,ofile,args.reg_build,url)
        print (cmd)
        (res,sto,ste) = execCmd(cmd)
        nowEnd=time.time()
        msg = "done"
        if res != 0:
            msg = "error"
            ret = int(res)

        db.updateFileFilter(((msg,nowStart,ofile,nowEnd,res,sto,ste,fid),))
        print ("%s producing filtered %s " % (msg,ofile))

    return ret

def run(parser,args):
    args.out_dir = os.path.abspath(args.out_dir)
    db_file=args.db_file
    db = chipqc_db.ChipQcDbSqlite(path=db_file)
    if args.skip:
        skipFilter(db,args)
    elif 'reg_build' not in args or args.reg_build is None:
        print ("Regulatory build parameter required ")
        return 1
    else:
        ret = filter(db,args)
        if ret != 0:
            print ("Error: there were errors during execution !!!")
    return 1