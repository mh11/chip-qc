__author__ = 'mh719'
# http://blog.nextgenetics.net/?e=42

import sqlite3 as lite
from numpy import *
from matplotlib.mlab import PCA
import pygal


def _getCorrData(cur):
    cur.execute("SELECT d1.did,d2.did,c FROM foo f, data_file d1, data_file d2 WHERE d1.external_id = f.a and d2.external_id = f.b")
    data = cur.fetchall()
    return data

def _doCorrelation(data):
    cnt = 0
    idDict = {}
    for row in data:
        if row[0] not in idDict.keys():
            idDict[row[0]] = cnt
            cnt += 1

        if row[1] not in idDict.keys():
            idDict[row[1]] = cnt
            cnt += 1

    print "Found %s ids " % cnt

    mtx=empty(cnt*cnt,dtype=float64).reshape((cnt,cnt))
    mtx.fill(0)

    print "Initialized ..."

    for row in data:
        a = idDict[row[0]]
        b = idDict[row[1]]
        val = float(row[2])
        mtx[a,b] = val
        mtx[b,a] = val
    for i in range(cnt):
        mtx[i,i] = 1

    print "Done load values ..."
    results = PCA(mtx)
    print "PCA results ... "
    print "INFO: %s %s %s %s" % (str(len(results.Y)),str(len(results.Y[0])),str(len(results.fracs)), str(len(results.Wt)))
    print "Dim: %s, %s " % (str(results.numrows),str(results.numcols))
#    print results.Wt[0],results.Wt[1]

    xy_chart = pygal.XY(stroke=False)
    xy_chart.title = 'Correlation'
    xy_chart.add('a',[ (results.Wt[0][i],results.Wt[1][i]) for i in range(cnt-1)])
    #xy_chart.render()
    xy_chart.render_to_file('corr.svg')


def analyseCorrelation(args):
    db_file=args.db_file
    with lite.connect(db_file,timeout=30.0) as con:
        cur = con.cursor()
        corrData = _getCorrData(cur)

        _doCorrelation(corrData)


    return None