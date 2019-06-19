#info about tikv
tikv_ip="192.168.1.151"
tikv_port="20160"

# formula of calculating target metric
# eg: 0.4*throughput+0.6*latency
target_metric_set={"write_throughput":"1"}

# knobs to be tuned
target_knob_set=["block_cache_size"]



