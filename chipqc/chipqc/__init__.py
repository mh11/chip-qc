__author__ = 'mh719'
__version__ = '0.0.1'


import sys
import getopt
import argparse
import load_samples
import os.path

#def loadSamples(argsv):

def create_parser():
    """
    Generates a command line parser with args.
    :return: parser
    """
    parser = argparse.ArgumentParser(
        description='ChIP-seq QC package',
        epilog='chipqc <cmd> -h'
    )

#    parser.add_argument('CMD', type=str,choices=['load'], default=None, help="Type of program to run");
    parser.add_argument('-d','--db',type=str,dest='db_file', default='chip-seq-qc.db',help="Database file for analysis")

    # parser.add_argument('-f', '--infile', nargs='?', type=str, dest='in_file', help='Specify the input file')
    # parser.add_argument('-d', '--indir', nargs='?', type=str, dest='in_dir', help='Specify the input directory')
    # parser.add_argument('-o', '--outdir', nargs='?', type=str, dest='out_dir', help='Specify the output directory')
    # parser.add_argument('-g', '--gen', nargs='?', type=str, choices=['all', 'table', 'process'], dest='gen_code',
    #                     default='all', help='Generate new test code')
    # parser.add_argument('-e', '--exec', dest='exec_code', default=False, action='store_true',
    #                     help='Execute test code')
    # parser.add_argument('-t', '--test', dest='test_run', default=False, action='store_true',
    #                     help='Run app as tests.  Does not persist the generated or executed code.')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.1 - SNAPSHOT')

    subparsers = parser.add_subparsers( title="Sub commands", description="valid sub commands", help='sub-command help', dest="sub_cmd")

    # load sub command
    parser_load = subparsers.add_parser('load', help='load sample information')
    parser_load.add_argument('-m','--manifest',type=str,dest='mf_file',required=True,help="Manifest file with the following headers: DB_ID, EXP_ID,SAMPLE_ID,SAMPLE_NAME,EXP_TYPE,CELL_TYPE,FILE")

    # filter wiggs sub command
    parser_load = subparsers.add_parser('filter', help='Filter wiggle files')
    parser_load.add_argument('-o','--out-dir',type=str,dest='out_dir',default='wiggle-filtered',help="Output directory of the filtered wiggle file")

    # subparsers = parser.add_subparsers(title="Sub Processes",
    #                                    description="Sub-processes for etlTest",
    #                                    dest='subparser_name')
    # subparsers.required = False

    return parser

def main(argv=None):
    print "Main init"
    parser = create_parser()

    args, extra = parser.parse_known_args()

    print args
    print extra

    args.db_file = os.path.abspath(args.db_file)

    print "Using DB %s " % (args.db_file,)

    if args.sub_cmd == 'load':
        args.mf_file = os.path.abspath(args.mf_file)
        load_samples.load(args)


if __name__ == "__main__":
    main()

