__author__ = 'mh719'

import sqlite3 as lite
import os
import time
import sys
from exec_util import execCmd

def getHelpInfo():
    return "[OPTIONAL STEP] Filter wiggle files"

def addArguments(parser):
    parser.add_argument('-s','--skip',dest='skip',help="Skip filtering - use original files",action='store_true')
    parser.add_argument('-r','--regulatory-build',type=str,dest='reg_build',help="RegBuild.bb file")
    parser.add_argument('-o','--out-dir',type=str,dest='out_dir',default='wiggle-filtered',help="Output directory of the filtered wiggle file")
    parser.add_argument('-f','--file-id',type=int,dest='filter_id',help="File id to process - default all files are processed")


def skipFilter(args):
    db_file=args.db_file
    out_dir = args.out_dir
    filterid = list()
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)


## Store in DB
    with lite.connect(db_file,timeout=30.0) as con:
        cur = con.cursor()

        if 'filter_id' in args and args.filter_id != None:
            filterid.append(args.filter_id)
        else:
            cur.execute("SELECT id from filter")
            res = cur.fetchall()
            for row in res:
                filterid.append(int(row[0]))

        print filterid

        for fid in filterid:
            print "Copy %s over" % (fid,)
            cur.execute("SELECT exp_id,file from exp WHERE id = ? ",(fid,))
            (exp_id,url) = cur.fetchall()[0]
            now=time.time()
            cur.execute("UPDATE filter SET status=?, started=?,f_file_path=?,finished=?,exit_code=? WHERE id = ?",
                        ("done",now,url,now,0,fid))
            con.commit()


def filter(args):
    db_file=args.db_file
    out_dir = args.out_dir
    filterid = list()

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)


## Store in DB
    with lite.connect(db_file,timeout=30.0) as con:
        cur = con.cursor()

        if 'filter_id' in args and args.filter_id != None:
            filterid.append(args.filter_id)
        else:
            cur.execute("SELECT did FROM filter WHERE status = 'init'")
            res = cur.fetchall()
            for row in res:
                filterid.append(int(row[0]))

        for fid in filterid:
            cur.execute("SELECT did,data_file from data_file WHERE did = ? ",(fid,))
            (exp_id,url) = cur.fetchall()[0]
            cur.execute("SELECT exit_code,status from filter WHERE did = ? ",(fid,))
            (curr_code,curr_status) = cur.fetchall()[0]
            ofile=out_dir+"/"+str(exp_id)+".filtered.wig"

            print("For %s found %s using url %s to store as %s ..." % (fid,exp_id,url,ofile))

            if curr_code is not None:
                print("Already processed finished with %s" % curr_code)
                if curr_code == "0" or curr_code == 0:
                    print ("Already downloaded - done")
                    exit(0)
                else:
                    if os.path.isfile(ofile):
                        os.unlink(ofile) # clean up

#            oerr=out_dir+"/"+exp_id+".filtered.err"
#            olog=out_dir+"/"+exp_id+".filtered.log"
            now=time.time()
            cur.execute("UPDATE filter SET status=?, started=?,f_file_path=? WHERE did = ?",("started",now,ofile,fid))
            con.commit()

            cmd="%s write %s mult %s %s" % (args.wig_tool,ofile,args.reg_build,url)
            (res,sto,ste) = execCmd(cmd)
            now=time.time()
            cur.execute("UPDATE filter SET status=?, finished=?,exit_code=? WHERE did = ?",("done",now,res,fid))
            con.commit()


def run(parser,args):
    args.out_dir = os.path.abspath(args.out_dir)
    if args.skip:
        skipFilter(args)
    elif 'reg_build' not in args:
        print ("Regulatory build parameter missing")
        return 1
    else:
        filter(args)
    return 0