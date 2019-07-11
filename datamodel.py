from settings import target_metric_name, target_knob_set
from controller import metric_set, knob_set
import numpy as np

class GPDataSet:
    #dataset for gpmodel
    def __init__(self):
        self.previous_rowlabels = None          #
        self.previous_knob_set = None           # correspond to mapped_workload_knob_dataset     [num of samples * num of knobs]
        self.previous_metric_set = None         # correspond to mapped_workload_metric_dataset   [num of samples * num of metrics]
                                                # value of metric/knob #j in sample *i
                                                # we assume all workloads are in the same type (leave out workload mapping)

        self.knob_labels = None
        self.metric_labels = None

        self.new_rowlabels = None
        self.new_knob_set = None                # [num of knobs]
        self.new_metric_set = None              # [num of metrics]
                                                # value of metric/knob #j in sample *i

        #important_knobs                        # we assume all selected knobs are important (leave out clustering)

        self.target_metric = None               # name of target metric
        self.target_knobs = None                # name of target knobs
        self.target_lessisbetter = None         # whether less value of target metric is better

        self.num_knobs = None
        self.num_metrics = None
        self.num_previousamples = None

    def add_new_data(self, new_knob_list, new_metric_list):
        self.new_knob_set = new_knob_list
        self.new_metric_set = new_metric_list

    def merge_new_data(self):
        self.previous_metric_set = np.vstack((self.previous_metric_set, self.new_metric_set))
        self.previous_knob_set = np.vstack((self.previous_knob_set, self.new_knob_set))
        self.num_previousamples+=1
        self.previous_rowlabels = [x + 1 for x in range(self.num_previousamples)]

    def initdataset(self, metric_list):
        self.num_knobs = len(knob_set.keys())
        self.num_metrics = len(metric_list)
        self.num_previousamples = 0

        self.knob_labels = np.array([x for x in knob_set.keys()])
        self.metric_labels = np.array([x for x in metric_list])

        self.previous_rowlabels = [x+1 for x in range(self.num_previousamples)]
        self.previous_knob_set = np.zeros([self.num_previousamples, self.num_knobs])
        self.previous_metric_set = np.zeros([self.num_previousamples, self.num_metrics])

        self.new_rowlabels = [1]
        self.new_knob_set = np.zeros([1, self.num_knobs])
        self.new_metric_set = np.zeros([1, self.num_metrics])

        self.target_metric = target_metric_name
        self.target_knobs = target_knob_set
        self.target_lessisbetter = metric_set[target_metric_name]['lessisbetter']

        #training knob_set = np.vstack((self.previous_knob_set, self.new_knob_set))
        #training metric_set = np.vstack((self.previous_metric_set, self.new_metric_set))
        #predicting knob =

    def printdata(self):
        print("################## data ##################")
        print("------------------------------previous:------------------------------")
        print("knobs:  ", self.previous_knob_set)
        print("metrics:  ", self.previous_metric_set)
        print("rowlabels:  ", self.previous_rowlabels)
        print("num:  ", self.num_previousamples)
        print("------------------------------new:------------------------------")
        print("knobs:  ", self.new_knob_set)
        print("metrics:  ", self.new_metric_set)
        print("rowlabels:  ", self.new_rowlabels)
        print("------------------------------TARGET:------------------------------")
        print("knob:  ", self.target_knobs)
        print("metric:  ", self.target_metric)
        print("metric_lessisbetter:  ", self.target_lessisbetter)
        print("------------------------------------------------------------")
        print("num of knobs == ", self.num_knobs)
        print("knobs:  ", self.knob_labels)
        print("num of metrics == ", self.num_metrics)
        print("metrics:  ", self.metric_labels)
        print("------------------------------------------------------------")
        print("################## data ##################")






