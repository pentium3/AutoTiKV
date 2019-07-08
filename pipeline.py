from controller import read_metric, read_knob, set_knob, metric_set, knob_set, init_knobs, load_workload, run_workload, calc_metric
from gpmodel import configuration_recommendation
from datamodel import GPDataSet
from settings import tikv_ip, tikv_port, target_knob_set, target_metric_name, workload_set
import numpy as np


if __name__ == '__main__':
    ds = GPDataSet()
    ds.initdataset()
    Round=50
    init_knobs()
    load_workload("writeheavy")

    while(Round>0):
        num_knobs = len(knob_set.keys())
        num_metrics = len(metric_set.keys())
        new_knob_set = np.zeros([1, num_knobs])
        new_metric_before = np.zeros([1, num_metrics])
        new_metric_after = np.zeros([1, num_metrics])

        for i,x in enumerate(metric_set.keys()):
            new_metric_before[0][i] = read_metric(x)

        for i,x in enumerate(knob_set.keys()):
            new_knob_set[0][i] = read_knob(x)

        run_workload("writeheavy")

        for i,x in enumerate(metric_set.keys()):
            new_metric_after[0][i] = read_metric(x)

        new_metric = calc_metric(new_metric_after, new_metric_before, metric_set)

        ds.add_new_data(new_knob_set, new_metric)

        import pickle
        import time
        fp = "ds_"+str(time.time())+".pkl"
        with open(fp, "wb") as f:
            pickle.dump(ds, f)

        rec = configuration_recommendation(ds)

        ds.merge_new_data()

        #rec=rec['recommendation']

        for x in rec.keys():
            set_knob(x, rec[x])

        print("Round: ", Round, rec)
        ds.printdata()
        Round-=1

