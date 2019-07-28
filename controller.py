import sys
import os
from settings import tikv_ip, tikv_port, tikv_pd_ip, ycsb_port
import psutil
import numpy as np

#MEM_MAX = psutil.virtual_memory().total
MEM_MAX = 0.8*32*1024*1024*1024                 # memory size of tikv node, not current PC


#------------------knob controller------------------

# block_cache_size
def set_block_cache_size(ip, port, val):
    #./tikv-ctl --host 192.168.1.104:20160 modify-tikv-config -m storage -n block_cache.capacity -v 3072MB
    cmd="./tikv-ctl --host "+ip+":"+port+" modify-tikv-config -m storage -n block_cache.capacity -v "+str(val)+"MB"
    res=os.popen(cmd).read()                        # will return "success"
    return(res)

def read_block_cache_size(ip, port, knob_cache):
    #./tikv-ctl --host 192.168.1.104:20160 metrics | grep "tikv_config_rocksdb{cf=\"default\",name=\"block_cache_size\"}"
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics | grep \'tikv_config_rocksdb{cf=\"default\",name=\"block_cache_size\"}\''
    res=os.popen(cmd).read()
    res=int(res.split(' ')[1])
    res=int(res/1024/1024)                          # bytes to MB
    return(res)

# write_buffer_size
def set_write_buffer_size(ip, port, val):
    val=val*1024*1024                               # MB to bytes
    cmd="./tikv-ctl --host "+ip+":"+port+" modify-tikv-config -m kvdb -n default.write_buffer_size -v "+str(val)
    res=os.popen(cmd).read()                        # will return "success"
    return(res)

def read_write_buffer_size(ip, port, knob_cache):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics | grep \'tikv_config_rocksdb{cf=\"default\",name=\"write_buffer_size\"}\''
    res=os.popen(cmd).read()
    res=float(res.split(' ')[1])
    res=int(res/1024/1024)                          # bytes to MB
    return(res)

# delayed_write_rate
def set_delayed_write_rate(ip, port, val):
    cmd="./tikv-ctl --host "+ip+":"+port+" modify-tikv-config -m kvdb -n delayed_write_rate -v "+str(val)+"MB"
    res=os.popen(cmd).read()                        # will return "success"
    return(res)

def read_delayed_write_rate(ip, port, knob_cache):
    res=knob_cache['delayed_write_rate']
    return(res)

# target_file_size_base
def set_target_file_size_base(ip, port, val):
    val=val*1024*1024                               # MB to bytes
    cmd="./tikv-ctl --host "+ip+":"+port+" modify-tikv-config -m kvdb -n default.target_file_size_base -v "+str(val)
    res=os.popen(cmd).read()                        # will return "success"
    return(res)

def read_target_file_size_base(ip, port, knob_cache):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics | grep \'tikv_config_rocksdb{cf=\"default\",name=\"target_file_size_base\"}\''
    res=os.popen(cmd).read()
    res=int(res.split(' ')[1])
    res=int(res/1024/1024)                          # bytes to MB
    return(res)

# disable_auto_compactions
def set_disable_auto_compactions(ip, port, val):
    cmd="./tikv-ctl --host "+ip+":"+port+" modify-tikv-config -m raftdb -n default.disable_auto_compactions -v "+str(val)
    res=os.popen(cmd).read()                        # will return "success"
    return(res)

def read_disable_auto_compactions(ip, port, knob_cache):
    cmd='./tikv-ctl --host '+ip+':'+port+' metrics | grep \'tikv_config_rocksdb{cf=\"default\",name=\"disable_auto_compactions\"}\''
    res=os.popen(cmd).read()
    res=int(res.split(' ')[1])
    return(res)

knob_set=\
    {"block_cache_size":
        {
            "read_func": read_block_cache_size,
            "set_func": set_block_cache_size,
            "minval": 0,                                # if type!=enum, indicate min possible value
            "maxval": 0,                             # if type!=enum, indicate max possible value
            "enumval": [],                              # if type==enum, list all valid values
            "type": "int",                              # int / enum / real / bool
            "default": 0                              # default value
        },
    "write_buffer_size":
        {
            "read_func": read_write_buffer_size,
            "set_func": set_write_buffer_size,
            "minval": 64,                           # if type!=enum, indicate min possible value
            "maxval": 1024,                         # if type!=enum, indicate max possible value
            "enumval": [],                          # if type==enum, list all valid values
            "type": "int",                          # int / enum / real / bool
            "default": 64                           # default value
        },
    "delayed_write_rate":
        {
            "read_func": read_delayed_write_rate,
            "set_func": set_delayed_write_rate,
            "minval": 0,                            # if type!=enum, indicate min possible value
            "maxval": 100,                          # if type!=enum, indicate max possible value
            "enumval": [],                          # if type==enum, list all valid values
            "type": "int",                          # int / enum / real / bool
            "default": 1                            # default value
        },
    "target_file_size_base":
        {
            "read_func": read_target_file_size_base,
            "set_func": set_target_file_size_base,
            "minval": 0,                            # if type!=enum, indicate min possible value
            "maxval": 0,                            # if type!=enum, indicate max possible value
            "enumval": [8,16,32,64,128],            # if type==enum, list all valid values
            "type": "enum",                         # int / enum / real / bool
            "default": 8                            # default value
        },
    "disable_auto_compactions":
        {
            "read_func": read_disable_auto_compactions,
            "set_func": set_disable_auto_compactions,
            "minval": 0,                            # if type!=enum, indicate min possible value
            "maxval": 0,                            # if type!=enum, indicate max possible value
            "enumval": [0, 1],                      # if type==enum, list all valid values
            "type": "enum",                         # int / enum / real / bool
            "default": 0                            # default value
        },
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
    return(0)           # DEPRECATED FUNCTION: latency is instant and could be read from go-ycsb. No need to read in this function
    # cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    # res=os.popen(cmd).read()
    # reslist=res.split("\n")
    # ans1=0
    # ans2=1
    # ans3=0
    # ans4=1
    # for rl in reslist:
    #     if ('tikv_grpc_msg_duration_seconds_sum{type="kv_prewrite"}' in rl):
    #         ans1 = float(rl.split(' ')[1])
    #     if ('tikv_grpc_msg_duration_seconds_count{type="kv_prewrite"}' in rl):
    #         ans2 = float(rl.split(' ')[1])
    #     if ('tikv_grpc_msg_duration_seconds_sum{type="kv_commit"}' in rl):
    #         ans3 = float(rl.split(' ')[1])
    #     if ('tikv_grpc_msg_duration_seconds_count{type="kv_commit"}' in rl):
    #         ans4 = float(rl.split(' ')[1])
    # ans=(ans1/ans2)+(ans3/ans4)
    # return(ans)

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
    return(0)           # DEPRECATED FUNCTION: latency is instant and could be read from go-ycsb. No need to read in this function
    # cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    # res=os.popen(cmd).read()
    # reslist=res.split("\n")
    # ans1=0
    # ans2=1
    # for rl in reslist:
    #     if ('tikv_grpc_msg_duration_seconds_sum{type="kv_batch_get"}' in rl):
    #         ans1 = float(rl.split(' ')[1])
    #     if ('tikv_grpc_msg_duration_seconds_count{type="kv_batch_get"}' in rl):
    #         ans2 = float(rl.split(' ')[1])
    # ans=ans1/ans2
    # return(ans)

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
    return(0)           # DEPRECATED FUNCTION: latency is instant and could be read from go-ycsb. No need to read in this function
    # cmd='./tikv-ctl --host '+ip+':'+port+' metrics'
    # res=os.popen(cmd).read()
    # reslist=res.split("\n")
    # ans1=0
    # ans2=1
    # for rl in reslist:
    #     if ('tikv_grpc_msg_duration_seconds_sum{type="kv_scan"}' in rl):
    #         ans1 = float(rl.split(' ')[1])
    #     if ('tikv_grpc_msg_duration_seconds_count{type="kv_scan"}' in rl):
    #         ans2 = float(rl.split(' ')[1])
    # ans=ans1/ans2
    # return(ans)

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
         "calc": "inc",                       #incremental
         },
    "write_latency":
        {
         "read_func": read_write_latency,
         "lessisbetter": 1,                    # whether less value of this metric is better(1: yes)
         "calc": "ins",                       #instant
        },
    "get_throughput":
        {
         "read_func": read_get_throughput,
         "lessisbetter": 0,                   # whether less value of this metric is better(1: yes)
         "calc": "inc",                       #incremental
        },
    "get_latency":
        {
         "read_func": read_get_latency,
         "lessisbetter": 1,                   # whether less value of this metric is better(1: yes)
         "calc": "ins",                       #instant
        },
    "scan_throughput":
        {
         "read_func": read_scan_throughput,
         "lessisbetter": 0,                   # whether less value of this metric is better(1: yes)
         "calc": "inc",                       #incremental
        },
    "scan_latency":
        {
         "read_func": read_scan_latency,
         "lessisbetter": 1,                   # whether less value of this metric is better(1: yes)
         "calc": "ins",                       #instant
        },
    "store_size":
        {
         "read_func": read_store_size,
         "lessisbetter": 1,                   # whether less value of this metric is better(1: yes)
         "calc": "ins",                       #instant
        },
    "compaction_cpu":
        {
         "read_func": read_compaction_cpu,
         "lessisbetter": 1,                   # whether less value of this metric is better(1: yes)
         "calc": "inc",                       #incremental
        },
    }


#------------------workload controller------------------

def run_workload(wl_type):
    #./go-ycsb run tikv -P ./workloads/smallpntlookup -p tikv.pd=192.168.1.130:2379
    cmd="./go-ycsb run tikv -P ./workloads/"+wl_type+" -p tikv.pd="+tikv_pd_ip+':'+ycsb_port
    print(cmd)
    res=os.popen(cmd).read()
    return(res)

def load_workload(wl_type):
    #./go-ycsb load tikv -P ./workloads/smallpntlookup -p tikv.pd=192.168.1.130:2379
    cmd="./go-ycsb load tikv -P ./workloads/"+wl_type+" -p tikv.pd="+tikv_pd_ip+':'+ycsb_port
    print(cmd)
    res=os.popen(cmd).read()
    return(res)


#------------------common functions------------------

def set_knob(knob_name, knob_val):
    func=knob_set[knob_name]["set_func"]
    res=func(tikv_ip, tikv_port, knob_val)
    return res

def read_knob(knob_name, knob_cache):
    func=knob_set[knob_name]["read_func"]
    res=func(tikv_ip, tikv_port, knob_cache)
    return res

def read_metric(metric_name, rres=None):
    if(rres!=None):
        rl=rres.split('\n')
        rl.reverse()
        if(metric_name=="write_latency"):
            i=0
            while((not rl[i].startswith('UPDATE ')) and (not rl[i].startswith('INSERT '))):
                i+=1
            dat=rl[i][rl[i].find("Avg(us):") + 9:].split(",")[0]
            dat=int(dat)
            return(dat)
        elif(metric_name=="get_latency"):
            i=0
            while(not rl[i].startswith('READ ')):
                i+=1
            dat=rl[i][rl[i].find("Avg(us):") + 9:].split(",")[0]
            dat=int(dat)
            return(dat)
        elif(metric_name=="scan_latency"):
            i=0
            while(not rl[i].startswith('SCAN ')):
                i+=1
            dat=rl[i][rl[i].find("Avg(us):") + 9:].split(",")[0]
            dat=int(dat)
            return(dat)
    func=metric_set[metric_name]["read_func"]
    res=func(tikv_ip, tikv_port)
    return res

def init_knobs():
    # if there are knobs whose range is related to PC memory size, initialize them here
    knob_set["block_cache_size"]["maxval"]=int(MEM_MAX/1024/1024)        # (MB)
    knob_set["block_cache_size"]["default"]=512                          # a sample
    #knob_set["block_cache_size"]["maxval"] = 1024
    knob_set["block_cache_size"]["minval"] = 8

def calc_metric(metric_after, metric_before, metric_list):
    num_metrics = len(metric_list)
    new_metric = np.zeros([1, num_metrics])
    for i, x in enumerate(metric_list):
        if(metric_set[x]["calc"]=="inc"):
            new_metric[0][i]=metric_after[0][i]-metric_before[0][i]
        elif(metric_set[x]["calc"]=="ins"):
            new_metric[0][i]=metric_after[0][i]
    return(new_metric)


