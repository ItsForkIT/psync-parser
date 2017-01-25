# Script to find the connection and disconnection 

import pymongo as pm
import os
import numpy as np 
import datetime
import time
import json
import operator
import csv

try:
	connection = pm.MongoClient()
	db = connection.disarm_technoshine # switch to disarm database
except:
	print "Cannot connect to the mongo client. Please check the port address"

def analyse_file(file_path, path_to_name, nodes):
	data = [np.array(map(None, line.split(','))) for line in open(file_path)]

	index = file_path.rfind('/')
	path = file_path[:index]
	p = path_to_name[path]

	if p not in nodes:
		nodes[p] = {}

	for i in range(0, len(data)):
		if data[i][1]==' PEER_DISCOVERED':
			peer = data[i][2]
			peer = peer.strip()
			
			if peer not in nodes[p]:
				nodes[p][peer] = {}
				nodes[p][peer]["id"] = 0.0

			nodes[p][peer]["id"] = nodes[p][peer]["id"] + 1
			nodes[p][peer][nodes[p][peer]["id"]] = {}
			nodes[p][peer][nodes[p][peer]["id"]]["Connect"] = data[i][0]
		elif data[i][1]==' PEER_LOST':
			peer = data[i][2]
			peer = peer.strip()
			nodes[p][peer][nodes[p][peer]["id"]]["Disconnect"] = data[i][0]


def pretty(d, indent=0):
	for key, value in d.iteritems():
		print '\t' * indent + str(key)
		if isinstance(value, dict):
			pretty(value, indent+1)
		else:
			print '\t' * (indent+1) + str(value)

paths = ["./DB", "./DM", "./Gionee", "./KGEC_1", "./KGEC_2", 
		"./NITDGP_1", "./NITDGP_2", "./OnePlusOne"]

path_to_name = {}
for path in paths:
	if path[:4]=="./DB":
		path_to_name[path] = "DB"
	elif path[:4]=="./DM":
		path_to_name[path] = "DM"
	else:
		text_file = open(path + "/source.txt","r")
		path_to_name[path] = text_file.read()
		#print path_to_name

nodes = {}

for path in paths:
	count = 0;
	# get better way to get files
	for root, dirs, files in os.walk(path):
		for file in files:
			if(file.startswith("psyncLog")):
				count = count + 1
				print "================================================================================================"
				print "-----------------------Analysing ", path, "/", file, "--------------------------------"
				analyse_file(path + "/" + file, path_to_name, nodes)
	print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXFound ", count, " log files in ", path, "XXXXXXXXXXXXXXXXXXXXXXXXXX"

#pretty(nodes)
# print json.dumps(nodes,sort_keys=False,indent=4)

map_path = {
	"DB":"./DB/sync",
	"MCS":"./DB/sync",
	"DM":"./DM/sync", 
	"9434789009":"./Gionee/Working", 
	"8981068051":"./KGEC_1/Working", 
	"8017143352":"./KGEC_2/Working", 
	"7688062422":"./NITDGP_1/Working", 
	"9836242994":"./NITDGP_2/Working", 
	"9733120882":"./OnePlusOne/Working"
}
for path in map_path:
	count = 0
	

for node in nodes:
	if node not in map_path:
		print "Not in map path bitch"
		continue
	for nodeI in nodes[node]:
		if nodeI not in map_path:
			print "Not in map path bitch", nodeI
			continue
		for connection_number in nodes[node][nodeI]:
			if isinstance(nodes[node][nodeI][connection_number], dict):
				#print node, nodeI, connection_number 
				#print nodes[node][nodeI][connection_number]["Connect"]
				
				connection_time = nodes[node][nodeI][connection_number]["Connect"]
				connection_time = datetime.datetime.strptime(connection_time, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
				path = map_path[nodeI]
				found = False
				
				nodes[node][nodeI][connection_number]["location"] = {}

				for root, dirs, files in os.walk(path):
					for file in files:
						if(file.startswith("MapDisarm")):
							file_path = path + "/" + file

							data = [np.array(map(None, line.split(','))) for line in open(file_path)]
							for i in range(0,len(data)):
								if len(data[i])<6:
									continue
									
								date = data[i][5]
								date = date[:4] + "." + date[4:6] + "." + date[6:8] + "." + date[8:10] + "." + date[10:12] + "." + date[12:]
								date = date.rstrip()
								#print date
								try:
									log_time = datetime.datetime.strptime(date, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
									if log_time > connection_time:
										found = True
										break
									
								except:
									print " Date not in proper format", date
									# print file_path, "for", i
									continue

								nodes[node][nodeI][connection_number]["location"]["lat"] = data[i][0]
								nodes[node][nodeI][connection_number]["location"]["lon"] = data[i][1]
							
							if found:
								break
					if found:
						break
				
			else:
				print "Done"

# print json.dumps(nodes,sort_keys=False,indent=4)

start_time = "2016.08.19.10.00.00"
end_time = "2016.08.19.10.30.00"
start_time = datetime.datetime.strptime(start_time, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
end_time = datetime.datetime.strptime(end_time, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
# print start_time, end_time

d1 = start_time
d2 = start_time + datetime.timedelta(minutes=30)

interval = {}
while d2 <= end_time:
	# print d2
	d2str = d2.strftime('%Y.%m.%d.%H.%M.%S')
	interval[d2str] = {}
	for node in nodes:
		
		interval[d2str][node] = {}
		interval[d2str][node]["location"] = {}
		
		path = map_path[node]
		found = False

		for root, dirs, files in os.walk(path):
			for file in files:
				if(file.startswith("MapDisarm")):
					file_path = path + "/" + file

					data = [np.array(map(None, line.split(','))) for line in open(file_path)]
					for i in range(0,len(data)):
						if len(data[i])<6:
							continue
									
						date = data[i][5]
						date = date[:4] + "." + date[4:6] + "." + date[6:8] + "." + date[8:10] + "." + date[10:12] + "." + date[12:]
						date = date.rstrip()
						#print date
						try:
							log_time = datetime.datetime.strptime(date, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
							if log_time > d2:
								found = True
								break
									
						except:
							print " Date not in proper format", date
							# print file_path, "for", i
							continue

						interval[d2str][node]["location"]["lat"] = data[i][0]
						interval[d2str][node]["location"]["lon"] = data[i][1]
							
					if found:
						break
			if found:
				break

		for nodeI in nodes[node]:
			interval[d2str][node][nodeI] = {
				"time":0.0
			}
			for connection_number in nodes[node][nodeI]:
				if isinstance(nodes[node][nodeI][connection_number], dict):
					connection_time = nodes[node][nodeI][connection_number]["Connect"]
					connection_time = datetime.datetime.strptime(connection_time, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
					if connection_time >= d1 and connection_time <= d2 and "Disconnect" in nodes[node][nodeI][connection_number]:
						disconnection_time = nodes[node][nodeI][connection_number]["Disconnect"]
						disconnection_time = datetime.datetime.strptime(disconnection_time, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
						interval[d2str][node][nodeI]["time"] = interval[d2str][node][nodeI]["time"] + (disconnection_time - connection_time).total_seconds()
						if "location" in nodes[node][nodeI][connection_number]:
							interval[d2str][node][nodeI]["location"] = nodes[node][nodeI][connection_number]["location"]

	d2 = d2 + datetime.timedelta(minutes=30)

print json.dumps(interval,sort_keys=False,indent=4)

with open('result.csv', 'wb') as csv_file:
	writer = csv.writer(csv_file)
	for timestamp in interval:
		for node in interval[timestamp]:
			for nodeI in interval[timestamp][node]:
				writer.writerow([timestamp,node,nodeI,interval[timestamp][node][nodeI]])
		writer.writerow([])

with open('result.json', 'w') as fp:
    json.dump(interval, fp)