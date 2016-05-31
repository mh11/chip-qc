#from __future__ import print_function

__author__ = 'mh719'
__version__ = '0.1.0'


import sys
import argparse
import load_samples
import db_explore
import filter_samples
import correlate_files
import pca_analysis
import snapshot
import enrichment
import coverage_mean
import os
import inspect

# http://stackoverflow.com/questions/3718657/how-to-properly-determine-current-script-directory-in-python/22881871#22881871
def get_script_dir(follow_symlinks=True):
    if getattr(sys, 'frozen', False): # py2exe, PyInstaller, cx_Freeze
        path = os.path.abspath(sys.executable)
    else:
        path = inspect.getabsfile(get_script_dir)
    if follow_symlinks:
        path = os.path.realpath(path)
    return os.path.dirname(path)

def create_parser(subcmds):
    parser = argparse.ArgumentParser(
        description='ChIP-seq QC package',
        epilog='chipqc <cmd> -h',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('-d','--db',type=str,dest='db_file', default='chip-seq-qc.db',help="Database file for analysis")
    parser.add_argument('-v', '--version', action='version', version='%(prog)s version {0}'.format(__version__))

    subparsers = parser.add_subparsers( title="Sub commands", description="valid sub commands", help='sub-command help', dest="sub_cmd")

    for (key,imp) in subcmds:
        imp.addArguments(subparsers.add_parser(key,help = imp.getHelpInfo()))

    return parser

def main(argv=None):
#    print "Main init"
    cmdLst = [
        ('load',load_samples),
        ('list',db_explore),
        ('snapshot',snapshot),
        ('filter',filter_samples),
        ('correlate',correlate_files),
        ('pca',pca_analysis),
        ('mean-coverage',coverage_mean),
        ('enrichment',enrichment)]

    parser = create_parser(cmdLst)

    args, extra = parser.parse_known_args()
    args.db_file = os.path.abspath(args.db_file)

#    print("#Using DB {0} ".format(args.db_file))

    subcmds = { a:b for (a,b) in cmdLst }

    if args.sub_cmd in subcmds.keys():
        return subcmds[args.sub_cmd].run(parser,args)
    else:
        print ("Command %s not registered!!!"%args.sub_cmd)
        parser.print_help( )
    return 0

if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
