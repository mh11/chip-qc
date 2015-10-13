#from __future__ import print_function

__author__ = 'mh719'
__version__ = '0.0.1'


import sys
import argparse
import load_samples
import db_explore
import filter_samples
import correlate_files
import pca_analysis
import enrichment
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
        epilog='chipqc <cmd> -h'
    )

    parser.add_argument('-d','--db',type=str,dest='db_file', default='chip-seq-qc.db',help="Database file for analysis")
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.1 - SNAPSHOT')

    subparsers = parser.add_subparsers( title="Sub commands", description="valid sub commands", help='sub-command help', dest="sub_cmd")

    for (key,imp) in subcmds:
        imp.addArguments(subparsers.add_parser(key,help = imp.getHelpInfo()))

    return parser

def main(argv=None):
#    print "Main init"
    cmdLst = [
        ('load',load_samples),
        ('list',db_explore),
        ('filter',filter_samples),
        ('correlate',correlate_files),
        ('pca',pca_analysis),
        ('enrichment',enrichment)]

    parser = create_parser(cmdLst)

    args, extra = parser.parse_known_args()
    args.db_file = os.path.abspath(args.db_file)

#    print "Using DB %s " % (args.db_file,)

    subcmds = { a:b for (a,b) in cmdLst }

    if args.sub_cmd in subcmds.keys():
        subcmds[args.sub_cmd].run(parser,args)
    else:
        print ("Command %s not registered!!!"%args.sub_cmd)
        parser.print_help( )

if __name__ == "__main__":
    main()

