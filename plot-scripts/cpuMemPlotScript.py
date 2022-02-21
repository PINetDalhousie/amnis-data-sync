from pathlib import Path
import matplotlib.pyplot as plt
import argparse
import os
import glob

#LOGS_DIR = "logs/cpu-mem/"
X_AXIS_DENOM = 2  # logs are every 0.5s

def clear_existing_plot():
    # clear the previous figure
    plt.close()
    plt.cla()
    plt.clf()   

def plot_test_cpu_mem(test):             
    plot_cpu(log_dir + "/" + test + "-cpu.log")    
    plot_mem(log_dir + "/" + test + "-mem.log")

def plot_all_cpu_mem():
    # Loop through logs
    for root, dirs, files in os.walk(log_dir, topdown=True):
        for fname in files:
            if ".log" in fname:
                test_num = fname[:-8]
                plot_test_cpu_mem(test_num)

def plot_cpu(filename):
    line_x_axis = []
    cpu_y_axis = []
    title = Path(filename).stem

    with open(filename, "r") as f:
        for line_num, line in enumerate(f,1):       # to get the line number
            line_x_axis.append(line_num/X_AXIS_DENOM)         
            first_split = line.split(" us")
            cpu_y_axis.append(float(first_split[0]) )
    
    plt.plot(line_x_axis, cpu_y_axis)
    plt.xlabel('Seconds')
    plt.ylabel('CPU %')
    plt.title(title)    
    f = log_dir + "/" + title
    plt.savefig(f,bbox_inches="tight") 
    clear_existing_plot() 

def plot_mem(filename):
    line_x_axis = []
    mem_y_axis = []
    title = Path(filename).stem

    with open(filename, "r") as f:
        for line_num, line in enumerate(f,1):       # to get the line number
            line_x_axis.append(line_num/2)          # logs are every 0.5s
            first_split = line.split(" us")
            mem_y_axis.append(float(first_split[0]) )
    
    plt.plot(line_x_axis, mem_y_axis)
    plt.xlabel('Seconds')
    plt.ylabel('MiB')
    plt.title(title)
    f = log_dir + "/" + title
    plt.savefig(f,bbox_inches="tight")  
    clear_existing_plot()


parser = argparse.ArgumentParser(description='Script for plotting CPU and memory from test runs.')
parser.add_argument('--test', dest='test', type=str, help='Test that was run (ex 1-1)')
parser.add_argument('--log-dir', dest='log_dir', type=str, default="logs/cpu-mem", help='Results directory')

args = parser.parse_args()

test = args.test
log_dir = args.log_dir

# Remove existing plots
# plots = glob.glob(LOGS_DIR + "*")
# for f in plots:
#     if ".png" in f:
#         os.remove(f)

if not os.path.isdir(log_dir):
    os.makedirs(log_dir)

if test is not None:
    plot_test_cpu_mem(test)
else:
    plot_all_cpu_mem()
