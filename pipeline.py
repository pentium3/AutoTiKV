from controller import *

'''
PIPELINE:

while true:
    knob = read_knob()
    read_metric()
    run_workload()
    read_metric()
    new_knob = ML(knob, metric)
    set_knob(new_knob)


'''


set_knob("block_cache_size",555)
read_knob("block_cache_size")
read_metric("write_throughput")
run_workload("writeheavy")



