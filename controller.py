import sys
import os
from settings import tikv_ip, tikv_port

#------------------knob controller------------------

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

knob_set=\
    {"block_cache_size":
        {
        "read_func": read_block_cache_size,
        "set_func": set_block_cache_size,
        "minval": 0,                                # if type!=enum, indicate min possible value
        "maxval": 4,                                # if type!=enum, indicate max possible value
        "enumval": [],                              # if type==enum, list all valid values
        "type": "int",                              # int / enum / real
        "default": 10                               # default value
        }
    }



#------------------metric controller------------------

def read_write_throughput(ip, port):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    #res=os.popen(cmd).read()
    print(cmd)

def read_write_latency(ip, port):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    #res=os.popen(cmd).read()
    print(cmd)


metric_set=\
    {"write_throughput":
         {
         "read_func": read_write_throughput,
         "lessisbetter": "false",                   # whether less value of this metric is better
         },
    "write_latency":
        {
         "read_func": read_write_latency,
         "lessisbetter": "true",                    # whether less value of this metric is better
        }
    }



#------------------workload controller------------------

workload_set=["writeheavy", "pntlookup", "longscan", "shortscan"]

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



#------------------common functions------------------

def set_knob(knob_name, knob_val):
    func=knob_set[knob_name]["set_func"]
    func(tikv_ip, tikv_port, knob_val)

def read_knob(knob_name):
    func=knob_set[knob_name]["read_func"]
    func(tikv_ip, tikv_port)

def read_metric(metric_name):
    func=metric_set[metric_name]["read_func"]
    func(tikv_ip, tikv_port)

def init_knobs():
    # if there are knobs whose range is related to PC memory size, initialize them here
    pass


