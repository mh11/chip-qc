from numpy.f2py.rules import sepdict
__author__ = 'mh719'


from exec_util import execCmd



def analyseCorrelation(args):
    db_file=args.db_file
    r_file=args.r_file
    out_dir=args.out_dir

    cmdTemplate=" Rscript {0} --db {1} --out {2}"
    cmd=cmdTemplate.format(r_file,db_file,out_dir)
    res = execCmd(cmd)

#    start=res[0]
#    end=res[1]
#    exitVal = res[2]
#    out = res[3]
#    err = res[4]

    print (res)
    return None