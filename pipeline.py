from controller import read_metric, read_knob, set_knob, knob_set, init_knobs, load_workload, run_workload, calc_metric
from gpmodel import configuration_recommendation
from datamodel import GPDataSet
from settings import tikv_ip, tikv_port, target_knob_set, target_metric_name, workload_set, wl_metrics, wltype
import numpy as np


if __name__ == '__main__':
    ds = GPDataSet()
    Round=50
    init_knobs()
    metric_list=wl_metrics[wltype]
    ds.initdataset(metric_list)
    num_knobs = len(knob_set.keys())
    num_metrics = len(metric_list)

    while(Round>0):
        new_knob_set = np.zeros([1, num_knobs])
        new_metric_before = np.zeros([1, num_metrics])
        new_metric_after = np.zeros([1, num_metrics])

        for i,x in enumerate(metric_list):
            new_metric_before[0][i] = read_metric(x)

        for i,x in enumerate(knob_set.keys()):
            new_knob_set[0][i] = read_knob(x)

        lres = load_workload(wltype)
        rres = run_workload(wltype)
        #print(rres)

        for i,x in enumerate(metric_list):
            new_metric_after[0][i] = read_metric(x, rres)

        new_metric = calc_metric(new_metric_after, new_metric_before, metric_list)

        #print(new_metric,metric_list)

        ds.add_new_data(new_knob_set, new_metric)

        import pickle
        import time
        fp = "ds_"+str(time.time())+".pkl"
        with open(fp, "wb") as f:
            pickle.dump(ds, f)

        rec = configuration_recommendation(ds)

        ds.merge_new_data()

        for x in rec.keys():
            set_knob(x, rec[x])

        print("Round: ", Round, rec)
        ds.printdata()
        Round-=1

