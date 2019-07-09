#info about tikv
tikv_ip="192.168.1.151"
tikv_port="20160"
tikv_pd_ip="192.168.1.150"
ycsb_port="2379"


# target metric
target_metric_name="write_latency"

# knobs to be tuned
target_knob_set=["block_cache_size"]

# workloads to be run
workload_set=["writeheavy", "pntlookup", "longscan", "shortscan"]

wl_metrics={
    "writeheavy":["write_throughput","write_latency","store_size","compaction_cpu"],        #UPDATE
    "pntlookup": ["get_throughput","get_latency","store_size","compaction_cpu"],        #READ
    "longscan":  ["write_throughput","write_latency","scan_throughput","scan_latency","store_size","compaction_cpu"],        #SCAN, INSERT
    "shortscan": ["write_throughput","write_latency","scan_throughput","scan_latency","store_size","compaction_cpu"],        #SCAN, INSERT
}

wltype = "longscan"
