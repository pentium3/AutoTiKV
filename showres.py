
def showres(FL, OUTNAME):
    from datamodel import GPDataSet
    import pickle
    fo=open(OUTNAME, 'w')

    FNAME=FL[0]
    ff=open(FNAME,'rb')
    ds=pickle.load(ff)
    for j in ds.knob_labels:
        fo.write(j+', ')
    for j in ds.metric_labels:
        fo.write(j + ', ')
    fo.write("\n")
    ff.close()

    for FNAME in FL:
        ff=open(FNAME,'rb')
        ds=pickle.load(ff)
        #print(FNAME)
        for i in range(ds.num_previousamples):
            for j in (ds.previous_knob_set[i]):
                fo.write(str(float(j)) + ', ')
            for j in (ds.previous_metric_set[i]):
                fo.write(str(float(j))+', ')
            fo.write("\n")
        ff.close()
    fo.close()

