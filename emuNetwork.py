#!/usr/bin/python3

from mininet.topo import Topo
from mininet.node import OVSKernelSwitch, Host

import sys
import subprocess
import networkx as nx

class CustomTopo(Topo):
	def __init__(self, inputTopoFile):
		Topo.__init__(self)

		#Read topo information
		try:
			inputTopo = nx.read_graphml(inputTopoFile)
		except Exception as e:
			print("ERROR: Could not read topo properly.")
			print(str(e))
			sys.exit(1)

		self.seerSwitches = []
		self.devicePorts = {}	

		for node, data in inputTopo.nodes(data=True):
			if 'host_type' in data:
				type = data['host_type']
				if  type == 'SEER':
					switch = self.addSwitch(node,dpid=node[1], cls=OVSKernelSwitch, protocols='OpenFlow13')
					self.seerSwitches.append(switch)
				elif type == 'HOST':
					host = self.addHost(node, cls=Host)
				elif type == 'SWITCH':
					switch = self.addSwitch(node,dpid=node[1], cls=OVSKernelSwitch, failMode='standalone')
			
			elif node[0] == 'h':
				host = self.addHost(node, cls=Host)
			elif node[0] == 's':
				switch = self.addSwitch(node,dpid=node[1],cls=OVSKernelSwitch, failMode='standalone')
			else:
				print("ERROR: Wrong node identifier.")
				sys.exit(1)

		for source, target, data in inputTopo.edges(data=True):
			
			sPort = None
			if 'sport' in data:
				sPort = data['sport']
			if source not in self.devicePorts:
				self.devicePorts[source] = [sPort]
			else:
				self.devicePorts[source].append(sPort)

			dPort = None
			if 'dport' in data:
				dPort = data['dport']
			if target not in self.devicePorts:
				self.devicePorts[target] = [dPort]
			else:
				self.devicePorts[target].append(dPort)

			linkBandwidth = 1000
			if 'bandwidth' in data:
				linkBandwidth = int(data['bandwidth'])

			linkDelay = '1ms'
			if 'latency' in data:
				linkDelay = str(data['latency'])+'ms'

			#print("Link bandwidth: "+ str(linkBandwidth))
			#print("Link delay: "+ str(linkDelay))

			self.addLink(source, target, sPort, dPort, bw=linkBandwidth, delay=linkDelay)
		print(self.devicePorts)
			#self.addLink(source, target, data['sport'], data['dport'])



def configureNetwork(inputTopoFile):
	try:
		inputTopo = nx.read_graphml(inputTopoFile)
	except Exception as e:
		print("ERROR: Could not read topo properly.")
		print(str(e))
		sys.exit(1)

	directedTopo = inputTopo.to_directed()

	#Swap switch ports in reverse edges
	for u, v, data in directedTopo.edges(data=True):
		if not "reverse" in data.keys():
			directedTopo[v][u]['reverse'] = True
			tmpPort = directedTopo[v][u]['sport']
			directedTopo[v][u]['sport'] = directedTopo[v][u]['dport']
			directedTopo[v][u]['dport'] = tmpPort

	shortestPaths = dict(nx.all_pairs_shortest_path(directedTopo))

	configMap = {}

	for srcNode in inputTopo.nodes:
		if srcNode[0] == 's':
			configMap[srcNode] = {}

			for dstNode in inputTopo.nodes:
				if dstNode[0] == 'h':
					configMap[srcNode][dstNode] = False

	#Select only paths among end hosts
	for source in shortestPaths.keys():

		if source[0] == 'h':
			for target in shortestPaths[source].keys():
				if target[0] == 'h' and target != source:

					pathLength = len(shortestPaths[source][target])
					shortestPath = shortestPaths[source][target]

					#Iterate over switches
					for i in range(1, pathLength-1):
						outLink = directedTopo.get_edge_data(shortestPath[i], 
														  shortestPath[i+1])

						srcSwitch = shortestPath[i]

						#Check whether switch needs rule for a particular dst host
						if not configMap[srcSwitch][target]:

							#Install forwarding rule
							targetIP = target[1:]
							outPort = outLink['sport']

							ovsRule = "sudo ovs-ofctl add-flow "+srcSwitch+" ip,nw_dst=10.0.0."+targetIP+",actions=output:"+str(outPort)

							configMap[srcSwitch][target] = True

							#TODO: add error handler
							subprocess.Popen("sudo ovs-ofctl add-flow "+srcSwitch+" ip,nw_dst=10.0.0."+targetIP+",actions=output:"+str(outPort), shell=True)

	#print(configMap)
















