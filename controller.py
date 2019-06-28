import sys
import os
from settings import tikv_ip, tikv_port
import psutil

MEM_MAX = psutil.virtual_memory().total

#------------------knob controller------------------

def set_block_cache_size(ip, port, val):
    #./tikv-ctl --host 192.168.1.151:20160 modify-tikv-config -m storage -n block_cache.capacity -v 3072MB
    cmd="./tikv-ctl --host "+ip+":"+port+" modify-tikv-config -m storage -n block_cache.capacity -v "+str(val)+"MB"
    res=os.popen(cmd).read()                        # will return "success"
    return(res)

def read_block_cache_size(ip, port):
    #./tikv-ctl --host 192.168.1.151:20160 metrics | grep "tikv_config_rocksdb{cf=\"default\",name=\"block_cache_size\"}"
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics | grep "tikv_config_rocksdb{cf=\"default\",name=\"block_cache_size\"}"'
    res=os.popen(cmd).read()
    res=int(res/1024/1024)                          # in MB
    return(res)

knob_set=\
    {"block_cache_size":
        {
        "read_func": read_block_cache_size,
        "set_func": set_block_cache_size,
        "minval": 0,                                # if type!=enum, indicate min possible value
        "maxval": 4096,                             # if type!=enum, indicate max possible value
        "enumval": [],                              # if type==enum, list all valid values
        "type": "int",                              # int / enum / real / bool
        "default": 512                              # default value
        }
    }


#------------------metric controller------------------

def read_write_throughput(ip, port):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    res=os.popen(cmd).read()
    reslist=res.split("\n")
    ans0 =0
    for rl in reslist:
        if ('tikv_grpc_msg_duration_seconds_count{type="kv_prewrite"}' in rl):
            ans0 = int(rl.split(' ')[1])
            break
    return(ans0)

def read_write_latency(ip, port):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    res=os.popen(cmd).read()
    reslist=res.split("\n")
    ans1=0
    ans2=1
    ans3=0
    ans4=1
    for rl in reslist:
        if ('tikv_grpc_msg_duration_seconds_sum{type="kv_prewrite"}' in rl):
            ans1 = float(rl.split(' ')[1])
        if ('tikv_grpc_msg_duration_seconds_count{type="kv_prewrite"}' in rl):
            ans2 = float(rl.split(' ')[1])
        if ('tikv_grpc_msg_duration_seconds_sum{type="kv_commit"}' in rl):
            ans3 = float(rl.split(' ')[1])
        if ('tikv_grpc_msg_duration_seconds_count{type="kv_commit"}' in rl):
            ans4 = float(rl.split(' ')[1])
    ans=(ans1/ans2)+(ans3/ans4)
    return(ans)

def read_get_throughput(ip, port):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    res=os.popen(cmd).read()
    reslist=res.split("\n")
    ans0 =0
    for rl in reslist:
        if ('tikv_grpc_msg_duration_seconds_count{type="kv_batch_get"}' in rl):
            ans0 = int(rl.split(' ')[1])
            break
    return(ans0)

def read_get_latency(ip, port):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    res=os.popen(cmd).read()
    reslist=res.split("\n")
    ans1=0
    ans2=1
    for rl in reslist:
        if ('tikv_grpc_msg_duration_seconds_sum{type="kv_batch_get"}' in rl):
            ans1 = float(rl.split(' ')[1])
        if ('tikv_grpc_msg_duration_seconds_count{type="kv_batch_get"}' in rl):
            ans2 = float(rl.split(' ')[1])
    ans=ans1/ans2
    return(ans)

def read_scan_throughput(ip, port):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    res=os.popen(cmd).read()
    reslist=res.split("\n")
    ans0 =0
    for rl in reslist:
        if ('tikv_grpc_msg_duration_seconds_count{type="kv_scan"}' in rl):
            ans0 = int(rl.split(' ')[1])
            break
    return(ans0)

def read_scan_latency(ip, port):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    res=os.popen(cmd).read()
    reslist=res.split("\n")
    ans1=0
    ans2=1
    for rl in reslist:
        if ('tikv_grpc_msg_duration_seconds_sum{type="kv_scan"}' in rl):
            ans1 = float(rl.split(' ')[1])
        if ('tikv_grpc_msg_duration_seconds_count{type="kv_scan"}' in rl):
            ans2 = float(rl.split(' ')[1])
    ans=ans1/ans2
    return(ans)

def read_store_size(ip, port):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    res=os.popen(cmd).read()
    reslist=res.split("\n")
    ans0 =0
    for rl in reslist:
        if ('tikv_engine_size_bytes{db="kv",type="default"}' in rl):
            ans0 = int(rl.split(' ')[1])
            break
    return(ans0)

def read_compaction_cpu(ip, port):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    res=os.popen(cmd).read()
    reslist=res.split("\n")
    ans=0
    ans1=0
    for rl in reslist:
        if ('tikv_thread_cpu_seconds_total{name="rocksdb:low' in rl):
            ans1 = float(rl.split(' ')[1])
            ans+=ans1
    return(ans)

metric_set=\
    {"write_throughput":
         {
         "read_func": read_write_throughput,
         "lessisbetter": 0,                   # whether less value of this metric is better(1: yes)
         },
    "write_latency":
        {
         "read_func": read_write_latency,
         "lessisbetter": 1,                    # whether less value of this metric is better(1: yes)
        },
    "get_throughput":
        {
         "read_func": read_get_throughput,
         "lessisbetter": 0,                   # whether less value of this metric is better(1: yes)
        },
    "get_latency":
        {
         "read_func": read_get_latency,
         "lessisbetter": 1,                   # whether less value of this metric is better(1: yes)
        },
    "scan_throughput":
        {
         "read_func": read_scan_throughput,
         "lessisbetter": 0,                   # whether less value of this metric is better(1: yes)
        },
    "scan_latency":
        {
         "read_func": read_scan_latency,
         "lessisbetter": 1,                   # whether less value of this metric is better(1: yes)
        },
    "store_size":
        {
         "read_func": read_store_size,
         "lessisbetter": 1,                   # whether less value of this metric is better(1: yes)
        },
    "compaction_cpu":
        {
         "read_func": read_compaction_cpu,
         "lessisbetter": 1,                   # whether less value of this metric is better(1: yes)
        },

    }


#------------------workload controller------------------

def run_workload(wl_type):
    #./bin/go-ycsb run tikv -P workloads/workloada -p tikv.pd=192.168.1.150:2379
    cmd="./goycsb/bin/go-ycsb run tikv -P ./ycsb_workloads/"+wl_type+" -p tikv.pd="+tikv_ip+':'+tikv_port
    res=os.popen(cmd).read()

def load_workload(wl_type):
    #./bin/go-ycsb load tikv -P workloads/workloada -p tikv.pd=192.168.1.150:2379
    cmd="./goycsb/bin/go-ycsb load tikv -P ./ycsb_workloads/"+wl_type+" -p tikv.pd="+tikv_ip+':'+tikv_port
    res=os.popen(cmd).read()


#------------------common functions------------------

def set_knob(knob_name, knob_val):
    func=knob_set[knob_name]["set_func"]
    res=func(tikv_ip, tikv_port, knob_val)
    return res

def read_knob(knob_name):
    func=knob_set[knob_name]["read_func"]
    res=func(tikv_ip, tikv_port)
    return res

def read_metric(metric_name):
    func=metric_set[metric_name]["read_func"]
    res=func(tikv_ip, tikv_port)
    return res

def init_knobs():
    # if there are knobs whose range is related to PC memory size, initialize them here
    knob_set["block_cache_size"]["maxval"]=int(MEM_MAX/1024/1024)        # (MB)
    knob_set["block_cache_size"]["default"]=512                          # a sample



