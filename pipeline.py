from controller import *

set_knob("block_cache_size",555)
read_knob("block_cache_size")
read_metric("write_throughput")
run_workload("workloada")
