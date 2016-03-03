from numpy.f2py.rules import sepdict
__author__ = 'mh719'
# http://blog.nextgenetics.net/?e=42
# http://www.r-bloggers.com/pca-and-k-means-clustering-of-delta-aircraft/

from exec_util import execCmd

import os
import os.path
import inspect

def getHelpInfo():
    return "Principal component analysis (PCA) using Pearson correlation results"

def addArguments(parser):
    parser.add_argument('-o','--out-dir',type=str,dest='out_dir',default='%s/pca'%os.getcwd(),help="Output directory for PCA plots. [default:{0}]".format('%s/pca'%os.getcwd()))
    parser.add_argument('-A','--group-a',type=str,dest='annot_a',default="CELL_LINE",help="Provide annotation for first group. [default: CELL_LINE]")
    parser.add_argument('-B','--group-b',type=str,dest='annot_b',default="LIBRARY_STRATEGY",help="Provide annotation for second group. [default: LIBRARY_STRATEGY]")

def getScriptdir():
    return os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

def getRFile():
    cdir=getScriptdir() # script directory
    r_file="%s/pca_plot.R" % (cdir,)
    return r_file

def analyseCorrelation(args):
    db_file=args.db_file
    out_dir=args.out_dir
    annot_a=args.annot_a
    annot_b=args.annot_b
    r_script = getRFile()

    a_out_dir = "{0}/{1}".format(out_dir,annot_a)

    cmdTemplate = "Rscript {0} --key-a {1} --key-b {2} --db-file {3} --out-dir {4} "

    cmd = cmdTemplate.format(r_script,annot_a, annot_b, db_file, a_out_dir)
#    print(cmd)
    print("Write plots using {0} with {1} into directory {2}".format(annot_a,annot_b,a_out_dir))
    res = execCmd(cmd)
    exitVal = res[0]
    out = res[1]
    err = res[2]

    if exitVal != 0:
        print(out)
        print(err)
        raise Exception("PCA R script quit with erorr code {0} ".format(exitVal))

    print("Finished creating plots in {0}!".format(a_out_dir))

    return exitVal

def run(parser,args):
    analyseCorrelation(args)

