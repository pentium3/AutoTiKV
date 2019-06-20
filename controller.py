import sys
import os
from settings import tikv_ip, tikv_port

def set_block_cache_size(ip, port, val):
    #./tikv-ctl --host 192.168.1.151:20160 modify-tikv-config -m storage -n block_cache.capacity -v 10GB
    cmd="./tikv-ctl --host "+ip+":"+port+" modify-tikv-config -m storage -n block_cache.capacity -v "+str(val)+"GB"
    #res=os.popen(cmd).read()
    print(cmd)

def read_block_cache_size(ip, port):
    #./tikv-ctl --host 192.168.1.151:20160 metrics | grep "tikv_config_rocksdb{cf=\"default\",name=\"block_cache_size\"}"
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics | grep "tikv_config_rocksdb{cf=\"default\",name=\"block_cache_size\"}"'
    #res=os.popen(cmd).read()
    print(cmd)

def read_write_throughput(ip, port):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    #res=os.popen(cmd).read()
    print(cmd)

def read_write_latency(ip, port):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    #res=os.popen(cmd).read()
    print(cmd)

knob_set=\
    {"block_cache_size":
        {
        "read_func": read_block_cache_size, \
        "set_func": set_block_cache_size, \
        "range": [0,4], \
        "type": "continuous", \
        "default": 10
        }
    }

metric_set=\
    {"write_throughput":
         {
         "read_func": read_write_throughput, \
         },
    "write_latency":
        {
         "read_func": read_write_latency, \
        }
    }

workload_set=["writeheavy", "pntlookup", "longscan", "shortscan"]

def set_knob(knob_name, knob_val):
    func=knob_set[knob_name]["set_func"]
    func(tikv_ip, tikv_port, knob_val)

def read_knob(knob_name):
    func=knob_set[knob_name]["read_func"]
    func(tikv_ip, tikv_port)

def read_metric(metric_name):
    func=metric_set[metric_name]["read_func"]
    func(tikv_ip, tikv_port)

def run_workload(wl_type):
    #./bin/go-ycsb run tikv -P workloads/workloada -p tikv.pd=192.168.1.150:2379
    cmd="./goycsb/bin/go-ycsb run tikv -P ./ycsb_workloads/"+wl_type+" -p tikv.pd="+tikv_ip+':'+tikv_port
    #res=os.popen(cmd).read()
    print(cmd)

def load_workload(wl_type):
    #./bin/go-ycsb load tikv -P workloads/workloada -p tikv.pd=192.168.1.150:2379
    cmd="./goycsb/bin/go-ycsb load tikv -P ./ycsb_workloads/"+wl_type+" -p tikv.pd="+tikv_ip+':'+tikv_port
    #res=os.popen(cmd).read()
    print(cmd)
