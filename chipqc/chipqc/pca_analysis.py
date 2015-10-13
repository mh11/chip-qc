from numpy.f2py.rules import sepdict
__author__ = 'mh719'
# http://blog.nextgenetics.net/?e=42
# http://www.r-bloggers.com/pca-and-k-means-clustering-of-delta-aircraft/

import sqlite3 as lite
from numpy import *
from matplotlib.mlab import PCA
import pygal

def getHelpInfo():
    return "Principal component analysis (PCA) using Pearson correlation results"

def addArguments(parser):
    ## Don't do anything
    parser = parser

def _getCorrData(cur):
    cur.execute("SELECT d1.did,d2.did,c FROM foo f, data_file d1, data_file d2 WHERE d1.external_id = f.a and d2.external_id = f.b")
    data = cur.fetchall()
    return data

def _getAnnotation(cur):
    cur.execute("SELECT a.did,a.key,a.value FROM data_annotation a")
    data = cur.fetchall()
    annotDict = {}
    for row in data:
        id = row[0]
        if id not in annotDict.keys():
            annotDict[id] = {}
        annotDict[id][row[1]] = row[2]
    return annotDict


def _buildIndex(data):
    cnt = 0
    idDict = {}
    for row in data:
        if row[0] not in idDict.keys():
            idDict[row[0]] = cnt
            cnt += 1

        if row[1] not in idDict.keys():
            idDict[row[1]] = cnt
            cnt += 1

    print "Found %s ids " % (len(idDict))
    return idDict

def _buildMatrix(data,idIdx):
    cnt = len(idIdx)

    mtx=empty(cnt*cnt,dtype=float64).reshape((cnt,cnt))
    mtx.fill(0)

    print "Initialized ..."

    for row in data:
        a = idIdx[row[0]]
        b = idIdx[row[1]]
        val = float(row[2])
        mtx[a,b] = val
        mtx[b,a] = val
    for i in range(cnt):
        mtx[i,i] = 1

#    print "Shape %s  ... " % mtx.shape[0]
    return mtx


def _doCorrelation(idIdx,annot,mtx,group, separate=None):
    cnt = mtx.shape[0] # symmetric shaped matrix
    print "Done load values ..."
#    results = PCA(mtx)
    print "PCA results ... "
#    print "INFO: %s %s %s %s" % (str(len(results.Y)),str(len(results.Y[0])),str(len(results.fracs)), str(len(results.Wt)))
#    print "Dim: %s, %s " % (str(results.numrows),str(results.numcols))
#    print results.Wt[0],results.Wt[1]

    sepDict = {}
#    groupedSet = {}
#   x[(1,2,3),][:,(1,2,3)]
    for id,idx in idIdx.items():
        s = 'default'
        if separate is not None:
            s = annot[id][separate]
        g = annot[id][group]

        if s not in sepDict.keys():
            sepDict[s] = {}
        groupedSet = sepDict[s]
        if g not in groupedSet.keys():
            groupedSet[g] = []
        groupedSet[g].append(idx)

#        a = results.Wt[0][idx]
#        b = results.Wt[1][idx]
#
#        a = results.Wt[idx][0]
#        b = results.Wt[idx][1]
#        if s not in sepDict.keys():
#            sepDict[s] = {}
#        groupedSet = sepDict[s]
#        if g not in groupedSet.keys():
#            groupedSet[g] = []
#
#        groupedSet[g].append([a,b])

    for idx,sep in enumerate(sepDict.keys()):
        groupedSet = sepDict[sep]
        idset = []
        for x in groupedSet.values():
            idset.extend(x)

        idset = sorted(idset)

        idMapping = { y:x for x,y in enumerate(idset)}

        if len(idset) <= 1:
            print idset
        else:
            submtx = mtx[idset,:][:,idset]
            results = PCA(submtx)

            xy_chart = pygal.XY(stroke=False)
            xy_chart.title = sep

            for group in groupedSet.keys():
                lst = groupedSet[group]
                subIdxList = [idMapping[i]  for i in lst ]
#                res = [(results.Y[i][0],results.Y[i][1]) for i in subIdxList]
#                res = [(results.a[i][0],results.a[i][1]) for i in subIdxList]
#     ## wrong           res = [(results.Wt[i][0],results.Wt[i][1]) for i in subIdxList]
                res = [(results.Wt[0][i],results.Wt[1][i]) for i in subIdxList]
#                res = [(results.Wt[0][i]*-1,results.Wt[1][i]*-1) for i in subIdxList]
                xy_chart.add(group,res)
    #xy_chart.add('a',[ (results.Wt[0][i],results.Wt[1][i]) for i in range(cnt-1)])
        #xy_chart.render()
            name="plot"
            if separate is not None:
                name = separate
            xy_chart.render_to_file('{0}-{1}.svg'.format(separate,str(idx)))


def analyseCorrelation(args):
    db_file=args.db_file
    with lite.connect(db_file,timeout=30.0) as con:
        cur = con.cursor()
        corrData = _getCorrData(cur)
        annot = _getAnnotation(cur)

        idIdx = _buildIndex(corrData)
        mtx = _buildMatrix(corrData,idIdx)

        _doCorrelation(idIdx,annot,mtx,separate='EXPERIMENT_TYPE',group='CELL_TYPE')
        _doCorrelation(idIdx,annot,mtx,separate='CELL_TYPE',group='EXPERIMENT_TYPE')

    return None

def run(parser,args):
    analyseCorrelation(args)

