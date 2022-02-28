#!/usr/bin/python3

import subprocess
import os
import time
import sys

interval = 0.5

test = str(sys.argv[1])

if not os.path.isdir("logs/cpu-mem/"):
	os.system("sudo mkdir -p logs/cpu-mem/")

print("Monitoring CPU and memory. Press Ctrl+C to stop.")
while True:
	os.system("top -b -n 1 | head -n 3 | tail -n 1 | awk '{print $2}' >> logs/cpu-mem/" + test + "-cpu.log")
	os.system("top -b -n 1 | head -n 4 | tail -n 1 | awk '{print $8}' >> logs/cpu-mem/" + test + "-mem.log")
	time.sleep(interval)
