#info about tikv
tikv_ip="192.168.1.104"
tikv_port="20160"
tikv_pd_ip="192.168.1.104"
ycsb_port="2379"

# workloads and their related performance metrics
wl_metrics={
    "writeheavy":["write_throughput","write_latency","store_size","compaction_cpu"],        #UPDATE
    "pntlookup40": ["get_throughput","get_latency","store_size","compaction_cpu"],          #READ
    "pntlookup80": ["get_throughput","get_latency","store_size","compaction_cpu"],          #READ
    "longscan":  ["write_throughput","write_latency","scan_throughput","scan_latency","store_size","compaction_cpu"],        #SCAN, INSERT
    "shortscan": ["write_throughput","write_latency","scan_throughput","scan_latency","store_size","compaction_cpu"],        #SCAN, INSERT
    "smallpntlookup": ["get_throughput","get_latency","store_size","compaction_cpu"],       #READ
}

# workload to be run
wltype = "pntlookup80"

# only 1 target metric to be optimized
target_metric_name="get_latency"

# several knobs to be tuned
target_knob_set=['disable-auto-compactions', 'block-size']
#target_knob_set=['disable-auto-compactions', 'optimize-filters-for-hits', 'write-buffer-size', 'block-size', 'max-bytes-for-level-base']

ansibledir="/home/tidb/tidb-ansible/"
deploydir="/home/tidb/deploy/"