class GPDataSet:
    #dataset for gpmodel
    previous_knob_dataset       # correspond to mapped_workload_knob_dataset     [num of samples * num of knobs]
    previous_metric_dataset     # correspond to mapped_workload_metric_dataset   [num of samples * num of metrics]
                                # value of metric/knob #j in sample *i
                                # we assume all workloads are in the same type (leave out workload mapping)

    new_knob                    # [num of samples * num of knobs]
    new_metric                  # [num of samples * num of metrics]
                                # value of metric/knob #j in sample *i

    #important_knobs            # we assume all knobs are important (leave out clustering)

    name_of_target_metric       # name of target metric
    target_lessisbetter         # whether less value of target metric is better







