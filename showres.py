
def showres(FL):
    from datamodel import GPDataSet
    import pickle
    for FNAME in FL:
        ff=open(FNAME,'rb')
        ds=pickle.load(ff)
        #print(FNAME)
        for i in range(ds.num_previousamples):
            print(float(ds.previous_knob_set[i]), end=', ')
            for j in (ds.previous_metric_set[i]):
                print(float(j), end=', ')
            print("")
        ff.close()

