import requests
import json
import multiprocessing
import sys
import time
from random import seed, randint, choice, sample
from urllib3.exceptions import InsecureRequestWarning

def setup():
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

def submitSeerRequest(json_data, controllerIp):
	print("Submitting SEER Requests: " + json.dumps(json_data))
	url = "https://"+controllerIp+":8443/seer/requests"
	response = requests.post(url, auth=('onos', 'rocks'), verify=False, json=json_data)
	print("SEER Request Response Code: " + str(response.status_code))
	#print(response.content)
	if response.status_code != 200:
		print("Response Message: " + str(response.json()['message']))

def startSeerLimitRequests(net, args):
	setup()
	seerSwitches = net.topo.seerSwitches
	devicePorts = net.topo.devicePorts
	controllerIp = args.controllerIp
	seerLimitRate = args.seerLimitRate
	seerLimitTime = args.seerLimitTime
	while True:
		seerSwitch = choice(seerSwitches)
		deviceId = "of:000000000000000" + seerSwitch[1]
		seerSwitchPorts = devicePorts[seerSwitch]
		selectedPorts = sample(seerSwitchPorts, 2)
		json_data = {
  			"requests": [
   			{
  				"requestType": "LIMIT",
  				"deviceId": deviceId,
  				"seerInPorts": [
    				selectedPorts[0]
  				],
  				"seerOutPorts": [
    				selectedPorts[1]
  				],
				"timeout": seerLimitTime,
  				"flowRate": seerLimitRate
			}
  			]
		}
		submitSeerRequest(json_data, controllerIp)
		time.sleep(seerLimitTime)

def startSeerBlockRequests(net, args):
	setup()
	seerSwitches = net.topo.seerSwitches
	devicePorts = net.topo.devicePorts
	controllerIp = args.controllerIp
	seerBlockTime = args.seerBlockTime
	while True:
		seerSwitch = choice(seerSwitches)
		deviceId = "of:000000000000000" + seerSwitch[1]
		seerSwitchPorts = devicePorts[seerSwitch]
		selectedPorts = sample(seerSwitchPorts, 1)
		json_data = {
  			"requests": [
   			{
  				"requestType": "BLOCK",
  				"deviceId": deviceId,
  				"seerInPorts": [
    				selectedPorts[0]
  				],
				"timeout": seerBlockTime
			}
  			]
		}
		submitSeerRequest(json_data, controllerIp)
		time.sleep(seerBlockTime)

def deleteSeerRequests(args, seer_processes):
	for process in seer_processes:
		process.terminate()
	setup()
	print("Deleting SEER Requests")
	url = "https://" + args.controllerIp + ":8443/seer/requests"
	response = requests.delete(url, auth=('onos', 'rocks'), verify=False)
	print("Response Code: " + str(response.status_code))

def spawnSeerRequests(net, args):
	seer_processes = []

	if args.seerLimitTime > -1:
		process = multiprocessing.Process(target=startSeerLimitRequests, args=(net,args))
		process.start()
		seer_processes.append(process)
		print("Started SEER LIMIT Requests")
	
	if args.seerBlockTime > -1:
		process = multiprocessing.Process(target=startSeerBlockRequests, args=(net,args))
		process.start()
		seer_processes.append(process)
		print("Started SEER BLOCK Requests")

	return seer_processes