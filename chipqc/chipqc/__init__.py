__author__ = 'mh719'
__version__ = '0.0.1'


import sys
import argparse
import load_samples
import db_explore
import filter_samples
import correlate_files
import os.path

def create_parser():
    parser = argparse.ArgumentParser(
        description='ChIP-seq QC package',
        epilog='chipqc <cmd> -h'
    )

    parser.add_argument('-d','--db',type=str,dest='db_file', default='chip-seq-qc.db',help="Database file for analysis")
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.1 - SNAPSHOT')

    subparsers = parser.add_subparsers( title="Sub commands", description="valid sub commands", help='sub-command help', dest="sub_cmd")

    # load sub command
    parser_load = subparsers.add_parser('load', help='load sample information')
    parser_load.add_argument('-m','--manifest',type=str,dest='mf_file',required=True,help="Manifest file with the following headers: DB_ID, EXP_ID,SAMPLE_ID,SAMPLE_NAME,EXP_TYPE,CELL_TYPE,FILE")

    # list DB entries
    parser_list = subparsers.add_parser('list', help='list loaded information')
    parser_list.add_argument('-l','--loaded-files',dest='l_file',help="Print loaded files",action='store_true')
    parser_list.add_argument('-f','--filtered-files',dest='f_file',help="Print filtered files status",action='store_true')
    parser_list.add_argument('-c','--correlation',dest='c_list',help="Print Correlation job list with file names, status and output",action='store_true')
    parser_list.add_argument('-C','--correlation-by-sample-id',dest='C_list',help="Print Correlation job list with Sample Ids, status and output",action='store_true')
    parser_list.add_argument('-p','--details',type=int,dest='detail_id',help="Detailed information about job id")


    # filter wiggs sub command
    parser_load = subparsers.add_parser('filter', help='[OPTIONAL STEP] Filter wiggle files')
    parser_load.add_argument('-s','--skip',dest='skip',help="Skip filtering - use original files",action='store_true')
    parser_load.add_argument('-r','--regulatory-build',type=str,dest='reg_build',help="RegBuild.bb file")
    parser_load.add_argument('-o','--out-dir',type=str,dest='out_dir',default='wiggle-filtered',help="Output directory of the filtered wiggle file")
    parser_load.add_argument('-f','--file-id',type=int,dest='filter_id',help="File id to process - default all files are processed")
    parser_load.add_argument('-w','--wiggle-tool',type=str,dest='wig_tool',default="wiggletools",help="Set path to specific wiggle tool to use")


    # correlate files
    parser_load = subparsers.add_parser('correlate', help='Calculate Pearson correlation')
    parser_load.add_argument('-w','--wiggle-tool',type=str,dest='wig_tool',default="wiggletools",help="Set path to specific wiggle tool to use")
    parser_load.add_argument('-f','--force-all',dest='force',help="Force recalculation of values",action='store_true')
    parser_load.add_argument('-c','--correlation-id',type=int,dest='correlation_id',help="Correlation id to process - default: all correlations with missing values are run")
    parser_load.add_argument('-l','--limit',type=int,dest='limit',help="Limit numbers of jobs - default: no limitations")

    return parser

def main(argv=None):
#    print "Main init"
    parser = create_parser()

    args, extra = parser.parse_known_args()

#    print args
#    print extra

    args.db_file = os.path.abspath(args.db_file)

#    print "Using DB %s " % (args.db_file,)

    if args.sub_cmd == 'load':
        args.mf_file = os.path.abspath(args.mf_file)
        load_samples.load(args)
    elif args.sub_cmd == 'list':
        if args.f_file:
            db_explore.filterIds(args)
        elif args.l_file:
            db_explore.loadedIds(args)
        elif args.c_list:
            db_explore.correlationIds(args)
        elif args.C_list:
            db_explore.correlationSampleIds(args)
        elif 'detail_id' in args and args.detail_id != None:
            db_explore.correlationDetails(args,args.detail_id)
        else:
            print "One option required!!!"
            parser.print_help()
    elif args.sub_cmd == 'filter':
        args.out_dir = os.path.abspath(args.out_dir)
        if args.skip:
            filter_samples.skipFilter(args)
        elif 'reg_build' not in args:
            print ("Regulatory build parameter missing")
            return 1
        else:
            filter_samples.filter(args)
    elif args.sub_cmd == 'correlate':
        correlate_files.correlate(args)



if __name__ == "__main__":
    main()

