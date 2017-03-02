# Script to calculate latency and delivery probability

import pymongo as pm
import os
import numpy as np 
import datetime
import time
import operator
import math
import json
import csv
import copy
from math import radians, cos, sin, asin, sqrt

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    km = 6367 * c
    return km*1000.0

try:
	connection = pm.MongoClient()
	db = connection.mobicom # switch to mobicom database
except:
	print "Cannot connect to the mongo client. Please check the port address"

cursor = db.files.find()
msg_type = {
	"Food" : 0.0,
	"Health" : 0.0,
	"Shelter" : 0.0,
	"SocialShare" : 0.0,
	"Victim" : 0.0,
	"data_Food" : 0.0,
	"data_Health" : 0.0,
	"data_Shelter" : 0.0,
	"data_SocialShare" : 0.0,
	"data_Victim" : 0.0
}

msgs = {
	"num" : 0.0,
	"cat" : copy.deepcopy(msg_type),
	"data" : 0.0,
	"latency" : 0.0,
	"std_deviation" : 0.0
}

file_details = {
	"count":copy.deepcopy(msgs),	# store all files
	"SMS":copy.deepcopy(msgs),   	# store only SMS
	"VIDEO":copy.deepcopy(msgs),	# store only VIDEO
	"IMAGE":copy.deepcopy(msgs),	# store only IMAGE
	"TEXT":copy.deepcopy(msgs),		# store only TEXT
	"RECORDING": copy.deepcopy(msgs)# store only RECORDING
}

in_DB = copy.deepcopy(file_details) # store details of files which reached DB

not_in_DB = copy.deepcopy(file_details) # store details of files which did not reach DB

dist_in_DB = [copy.deepcopy(msgs) for k in range(0,15)]
dist_not_in_DB = [copy.deepcopy(msgs) for k in range(0,15)]

file_analysis = {}
source_not_found = 0.0

malfunction_group = ["8906222492", "9674967768", "9433075181", "7031583420", "7076302126",
					"9830505305", "7551047169", "8906507952", "7876044546", "9598453635", 
					"7031534911", "9563971757", "7076884054", "9434820751", "7549023798"]

for document in cursor:
	#print document
	if document["type"] == "GPS TRAIL": # ignore GPS trails 
		continue

	time_of_creation = document["DATE_OF_CREATION"]

	source = document["SOURCE"]
	name = document["NAME"]
	if source not in document:
		source_not_found = source_not_found + 1
		continue

	source_size = document[source]["size"]

	if ("LON_creation" in document[source] and document["DESTINATION"] in document and "LON_stop_download" in document[document["DESTINATION"]]) :
		lon1, lat1 = float(document[source]["LON_creation"]), float(document[source]["LAT_creation"])
		lon2, lat2 = float(document[document["DESTINATION"]]["LON_stop_download"]), float(document[document["DESTINATION"]]["LAT_stop_download"])
		dist = haversine( lon1, lat1, lon2, lat2)
		print dist , ' for ', lat1, ', ', lon1, ' file ', document["NAME"]
		
		# check if file has reached destination 
		if (document["DESTINATION"] == 'defaultMcs' and 'DB' in document and source_size == document["DB"]["size"]):
			# add file to dist_in_DB
			dist_in_DB[int(math.floor(dist/50.0))]["num"] += 1
			dist_in_DB[int(math.floor(dist/50.0))]["cat"][document["MSG_TYPE"]] += 1
			dist_in_DB[int(math.floor(dist/50.0))]["cat"]["data_" + document["MSG_TYPE"]] += source_size/1000000.0
			dist_in_DB[int(math.floor(dist/50.0))]["data"] += source_size/1000000.0
			time1 = document["DB"]["timestamp_stop_download"]
			end = datetime.datetime.strptime(time1, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
			time2 = time_of_creation[:4] + "." + time_of_creation[4:6] + "." + time_of_creation[6:8] + "." + time_of_creation[8:10] + "." + time_of_creation[10:12] + "." + time_of_creation[12:]
			start = datetime.datetime.strptime(time2, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
			dist_in_DB[int(math.floor(dist/50.0))]["latency"] += ((end - start).total_seconds()/60.0)
		elif (document["DESTINATION"] != 'DB' and document["DESTINATION"] in document and source_size == document[document["DESTINATION"]]["size"]):
			# add file to dist_in_DB
			dist_in_DB[int(math.floor(dist/50.0))]["num"] += 1
			dist_in_DB[int(math.floor(dist/50.0))]["cat"][document["MSG_TYPE"]] += 1
			dist_in_DB[int(math.floor(dist/50.0))]["cat"]["data_" + document["MSG_TYPE"]] += source_size/1000000.0
			dist_in_DB[int(math.floor(dist/50.0))]["data"] += source_size/1000000.0
			time1 = document[document["DESTINATION"]]["timestamp_stop_download"]
			end = datetime.datetime.strptime(time1, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
			time2 = time_of_creation[:4] + "." + time_of_creation[4:6] + "." + time_of_creation[6:8] + "." + time_of_creation[8:10] + "." + time_of_creation[10:12] + "." + time_of_creation[12:]
			start = datetime.datetime.strptime(time2, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
			dist_in_DB[int(math.floor(dist/50.0))]["latency"] += ((end - start).total_seconds()/60.0)
		elif (document["DESTINATION"] != 'DB' and document["DESTINATION"] in malfunction_group):
			added = False
			for mal_node in malfunction_group:
				if (mal_node in document and mal_node != document["SOURCE"] and document[mal_node]["size"] == source_size):
					dist_in_DB[int(math.floor(dist/50.0))]["num"] += 1
					dist_in_DB[int(math.floor(dist/50.0))]["cat"][document["MSG_TYPE"]] += 1
					dist_in_DB[int(math.floor(dist/50.0))]["cat"]["data_" + document["MSG_TYPE"]] += source_size/1000000.0
					dist_in_DB[int(math.floor(dist/50.0))]["data"] += source_size/1000000.0
					time1 = document[mal_node]["timestamp_stop_download"]
					end = datetime.datetime.strptime(time1, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
					time2 = time_of_creation[:4] + "." + time_of_creation[4:6] + "." + time_of_creation[6:8] + "." + time_of_creation[8:10] + "." + time_of_creation[10:12] + "." + time_of_creation[12:]
					start = datetime.datetime.strptime(time2, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
					dist_in_DB[int(math.floor(dist/50.0))]["latency"] += ((end - start).total_seconds()/60.0)
					added = True
			if (added == False):
				# add file to dist_not_in_DB
				dist_not_in_DB[int(math.floor(dist/50.0))]["num"] += 1
				dist_not_in_DB[int(math.floor(dist/50.0))]["cat"][document["MSG_TYPE"]] += 1
				dist_not_in_DB[int(math.floor(dist/50.0))]["cat"]["data_" + document["MSG_TYPE"]] += source_size/1000000.0
				dist_not_in_DB[int(math.floor(dist/50.0))]["data"] += source_size/1000000.0
		else:
			# add file to dist_not_in_DB
			dist_not_in_DB[int(math.floor(dist/50.0))]["num"] += 1
			dist_not_in_DB[int(math.floor(dist/50.0))]["cat"][document["MSG_TYPE"]] += 1
			dist_not_in_DB[int(math.floor(dist/50.0))]["cat"]["data_" + document["MSG_TYPE"]] += source_size/1000000.0
			dist_not_in_DB[int(math.floor(dist/50.0))]["data"] += source_size/1000000.0

	nodes = []
	idx = 0
	for node in document:
		if node!="NAME" and node!="_id" and node!="DATE_OF_CREATION" and node!="SOURCE" and node!="type" and node!=document["SOURCE"]:
			if "timestamp_start_download" not in document[node]:
				# print "timestamp not in node "
				# print document, " node = ", node 
				continue
			else:
				document[node]["node"] = node
				nodes.insert(idx,document[node])
				idx = idx + 1

	
	nodes_sorted = sorted(nodes, key=operator.itemgetter('timestamp_start_download'))
	# print nodes_sorted

	flag = False
	time = 0.0
	destination = document["DESTINATION"]
	for i in range(0,len(nodes_sorted)):
		time = time + nodes_sorted[i]["time_taken"]
		if(nodes_sorted[i]["node"]==document["DESTINATION"] and nodes_sorted[i]["size"]==source_size):
			flag = True
			destination = document["DESTINATION"]
			break
		elif (document["DESTINATION"] == 'defaultMcs' and nodes_sorted[i]["node"]=='DB' and nodes_sorted[i]["size"]==source_size):
			flag = True
			destination = 'DB'
			break
		elif (nodes_sorted[i]["size"]==source_size and nodes_sorted[i]["node"] in malfunction_group and document["DESTINATION"] in malfunction_group):
			flag = True 
			destination = nodes_sorted[i]["node"]
			break

	if flag==True:
		# store overall stats
		in_DB["count"]["num"] += 1
		in_DB["count"]["cat"][document["MSG_TYPE"]] += 1
		in_DB["count"]["cat"]["data_" + document["MSG_TYPE"]] += source_size/1000000.0
		in_DB["count"]["data"] += source_size/1000000.0

		# store stat for corresponding file type
		in_DB[document["type"]]["num"] += 1
		in_DB[document["type"]]["cat"][document["MSG_TYPE"]] += 1
		in_DB[document["type"]]["cat"]["data_" + document["MSG_TYPE"]] += source_size/1000000.0
		in_DB[document["type"]]["data"] += source_size/1000000.0
		

		file_analysis[document["NAME"]] = {}
		file_analysis[document["NAME"]]["time"] = time
		time_of_reach = document[destination]["timestamp_stop_download"]
		end = datetime.datetime.strptime(time_of_reach, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
		time_of_creation = time_of_creation[:4] + "." + time_of_creation[4:6] + "." + time_of_creation[6:8] + "." + time_of_creation[8:10] + "." + time_of_creation[10:12] + "." + time_of_creation[12:]
		start = datetime.datetime.strptime(time_of_creation, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
		file_analysis[document["NAME"]]["actual_time"] = end - start
		file_analysis[document["NAME"]]["actual_time_seconds"] = ((end - start).total_seconds()/60.0)
		file_analysis[document["NAME"]]["type"] = document["type"]
	else:
		# store overall stats
		not_in_DB["count"]["num"] += 1
		not_in_DB["count"]["cat"][document["MSG_TYPE"]] += 1
		not_in_DB["count"]["cat"]["data_" + document["MSG_TYPE"]] += source_size/1000000.0
		not_in_DB["count"]["data"] += source_size/1000000.0

		# store stat for corresponding file type
		not_in_DB[document["type"]]["num"] += 1
		not_in_DB[document["type"]]["cat"][document["MSG_TYPE"]] += 1
		not_in_DB[document["type"]]["cat"]["data_" + document["MSG_TYPE"]] += source_size/1000000.0
		not_in_DB[document["type"]]["data"] += source_size/1000000.0

time_seconds = 0.0

actual_time_seconds = 0.0
time_required = {
	"total":[],
	"SMS":[],
	"VIDEO":[],
	"IMAGE":[],
	"TEXT":[],
	"RECORDING":[]
}
idx = 0
for files in file_analysis:
	# print files,":",file_analysis[files]["time"],":",file_analysis[files]["actual_time"],":",file_analysis[files]["actual_time_seconds"]
	time_seconds = time_seconds + file_analysis[files]["time"]
	actual_time_seconds = actual_time_seconds + file_analysis[files]["actual_time_seconds"]
	time_required["total"].insert(idx,file_analysis[files]["actual_time_seconds"])
	time_required[file_analysis[files]["type"]].insert(len(time_required[file_analysis[files]["type"]]),file_analysis[files]["actual_time_seconds"])
	idx = idx + 1
	in_DB["count"]["latency"] += file_analysis[files]["actual_time_seconds"]
	in_DB[file_analysis[files]["type"]]["latency"] += file_analysis[files]["actual_time_seconds"]

latency_network = (actual_time_seconds)/len(file_analysis)

#print "AVERAGE LATENCY for node to node transfer", time_seconds/len(file_analysis)
#print actual_time_seconds/len(file_analysis)

print "Source not found for", source_not_found, "files"

# print "Details of files delivered", json.dumps(in_DB,sort_keys=False,indent=4)
# print "Details of files not delivered", json.dumps(not_in_DB,sort_keys=False,indent=4)

# print json.dumps(dist_in_DB,sort_keys=False,indent=4)

# print json.dumps(dist_not_in_DB,sort_keys=False,indent=4)

for files in file_analysis:
	in_DB["count"]["std_deviation"] += math.pow( (file_analysis[files]["actual_time_seconds"]-latency_network), 2)
	in_DB[file_analysis[files]["type"]]["std_deviation"] += math.pow( (file_analysis[files]["actual_time_seconds"]-latency_network), 2)
	
for keys in in_DB:
	in_DB[keys]["std_deviation"] = in_DB[keys]["std_deviation"]/in_DB[keys]["num"] if (in_DB[keys]["num"] != 0) else 0
	in_DB[keys]["std_deviation"] = math.sqrt(in_DB[keys]["std_deviation"])

with open('dp_lat.csv', 'wb') as csv_file:
	writer = csv.writer(csv_file)
	writer.writerow(['File Categories', 'Received Files', 'Received Data', 'Food', 'Food Data', 
					'Health', 'Health Data', 'Shelter', 'Shelter Data', 'SocialShare', 'SocialShare Data',
					'Victim', 'Victim Data', 'Failed Files', 'Failed Data', 'Food', 'Food Data', 
					'Health', 'Health Data', 'Shelter', 'Shelter Data', 'SocialShare', 'SocialShare Data',
					'Victim', 'Victim Data', 'Delivery Probability', 'Latency', 'Std Deviation'])

	writer.writerow(['Overall', in_DB["count"]["num"], in_DB["count"]["data"], 
					in_DB["count"]["cat"]["Food"], in_DB["count"]["cat"]["data_Food"],
					in_DB["count"]["cat"]["Health"], in_DB["count"]["cat"]["data_Health"],
					in_DB["count"]["cat"]["Shelter"], in_DB["count"]["cat"]["data_Shelter"],
					in_DB["count"]["cat"]["SocialShare"], in_DB["count"]["cat"]["data_SocialShare"],
					in_DB["count"]["cat"]["Victim"], in_DB["count"]["cat"]["data_Victim"],

					not_in_DB["count"]["num"], not_in_DB["count"]["data"], 
					not_in_DB["count"]["cat"]["Food"], not_in_DB["count"]["cat"]["data_Food"],
					not_in_DB["count"]["cat"]["Health"], not_in_DB["count"]["cat"]["data_Health"],
					not_in_DB["count"]["cat"]["Shelter"], not_in_DB["count"]["cat"]["data_Shelter"],
					not_in_DB["count"]["cat"]["SocialShare"], not_in_DB["count"]["cat"]["data_SocialShare"],
					not_in_DB["count"]["cat"]["Victim"], not_in_DB["count"]["cat"]["data_Victim"],

					in_DB["count"]["num"]/ (in_DB["count"]["num"] + not_in_DB["count"]["num"]),
					in_DB["count"]["latency"]/in_DB["count"]["num"] if (in_DB["count"]["num"]!=0) else 0,
					in_DB["count"]["std_deviation"] ])

	writer.writerow(['SMS', in_DB["SMS"]["num"], in_DB["SMS"]["data"], 
					in_DB["SMS"]["cat"]["Food"], in_DB["SMS"]["cat"]["data_Food"],
					in_DB["SMS"]["cat"]["Health"], in_DB["SMS"]["cat"]["data_Health"],
					in_DB["SMS"]["cat"]["Shelter"], in_DB["SMS"]["cat"]["data_Shelter"],
					in_DB["SMS"]["cat"]["SocialShare"], in_DB["SMS"]["cat"]["data_SocialShare"],
					in_DB["SMS"]["cat"]["Victim"], in_DB["SMS"]["cat"]["data_Victim"],

					not_in_DB["SMS"]["num"], not_in_DB["SMS"]["data"], 
					not_in_DB["SMS"]["cat"]["Food"], not_in_DB["SMS"]["cat"]["data_Food"],
					not_in_DB["SMS"]["cat"]["Health"], not_in_DB["SMS"]["cat"]["data_Health"],
					not_in_DB["SMS"]["cat"]["Shelter"], not_in_DB["SMS"]["cat"]["data_Shelter"],
					not_in_DB["SMS"]["cat"]["SocialShare"], not_in_DB["SMS"]["cat"]["data_SocialShare"],
					not_in_DB["SMS"]["cat"]["Victim"], not_in_DB["SMS"]["cat"]["data_Victim"],

					in_DB["SMS"]["num"]/ (in_DB["SMS"]["num"] + not_in_DB["SMS"]["num"]),
					in_DB["SMS"]["latency"]/in_DB["SMS"]["num"] if (in_DB["SMS"]["num"]!=0) else 0,
					in_DB["SMS"]["std_deviation"] ])

	writer.writerow(['VIDEO', in_DB["VIDEO"]["num"], in_DB["VIDEO"]["data"], 
					in_DB["VIDEO"]["cat"]["Food"], in_DB["VIDEO"]["cat"]["data_Food"],
					in_DB["VIDEO"]["cat"]["Health"], in_DB["VIDEO"]["cat"]["data_Health"],
					in_DB["VIDEO"]["cat"]["Shelter"], in_DB["VIDEO"]["cat"]["data_Shelter"],
					in_DB["VIDEO"]["cat"]["SocialShare"], in_DB["VIDEO"]["cat"]["data_SocialShare"],
					in_DB["VIDEO"]["cat"]["Victim"], in_DB["VIDEO"]["cat"]["data_Victim"],

					not_in_DB["VIDEO"]["num"], not_in_DB["VIDEO"]["data"], 
					not_in_DB["VIDEO"]["cat"]["Food"], not_in_DB["VIDEO"]["cat"]["data_Food"],
					not_in_DB["VIDEO"]["cat"]["Health"], not_in_DB["VIDEO"]["cat"]["data_Health"],
					not_in_DB["VIDEO"]["cat"]["Shelter"], not_in_DB["VIDEO"]["cat"]["data_Shelter"],
					not_in_DB["VIDEO"]["cat"]["SocialShare"], not_in_DB["VIDEO"]["cat"]["data_SocialShare"],
					not_in_DB["VIDEO"]["cat"]["Victim"], not_in_DB["VIDEO"]["cat"]["data_Victim"],

					in_DB["VIDEO"]["num"]/ (in_DB["VIDEO"]["num"] + not_in_DB["VIDEO"]["num"]),
					in_DB["VIDEO"]["latency"]/in_DB["VIDEO"]["num"] if (in_DB["VIDEO"]["num"]!=0) else 0,
					in_DB["VIDEO"]["std_deviation"] ])

	writer.writerow(['IMAGE', in_DB["IMAGE"]["num"], in_DB["IMAGE"]["data"], 
					in_DB["IMAGE"]["cat"]["Food"], in_DB["IMAGE"]["cat"]["data_Food"],
					in_DB["IMAGE"]["cat"]["Health"], in_DB["IMAGE"]["cat"]["data_Health"],
					in_DB["IMAGE"]["cat"]["Shelter"], in_DB["IMAGE"]["cat"]["data_Shelter"],
					in_DB["IMAGE"]["cat"]["SocialShare"], in_DB["IMAGE"]["cat"]["data_SocialShare"],
					in_DB["IMAGE"]["cat"]["Victim"], in_DB["IMAGE"]["cat"]["data_Victim"],

					not_in_DB["IMAGE"]["num"], not_in_DB["IMAGE"]["data"], 
					not_in_DB["IMAGE"]["cat"]["Food"], not_in_DB["IMAGE"]["cat"]["data_Food"],
					not_in_DB["IMAGE"]["cat"]["Health"], not_in_DB["IMAGE"]["cat"]["data_Health"],
					not_in_DB["IMAGE"]["cat"]["Shelter"], not_in_DB["IMAGE"]["cat"]["data_Shelter"],
					not_in_DB["IMAGE"]["cat"]["SocialShare"], not_in_DB["IMAGE"]["cat"]["data_SocialShare"],
					not_in_DB["IMAGE"]["cat"]["Victim"], not_in_DB["IMAGE"]["cat"]["data_Victim"],

					in_DB["IMAGE"]["num"]/ (in_DB["IMAGE"]["num"] + not_in_DB["IMAGE"]["num"]),
					in_DB["IMAGE"]["latency"]/in_DB["IMAGE"]["num"] if (in_DB["IMAGE"]["num"]!=0) else 0,
					in_DB["IMAGE"]["std_deviation"] ])

	writer.writerow(['TEXT', in_DB["TEXT"]["num"], in_DB["TEXT"]["data"], 
					in_DB["TEXT"]["cat"]["Food"], in_DB["TEXT"]["cat"]["data_Food"],
					in_DB["TEXT"]["cat"]["Health"], in_DB["TEXT"]["cat"]["data_Health"],
					in_DB["TEXT"]["cat"]["Shelter"], in_DB["TEXT"]["cat"]["data_Shelter"],
					in_DB["TEXT"]["cat"]["SocialShare"], in_DB["TEXT"]["cat"]["data_SocialShare"],
					in_DB["TEXT"]["cat"]["Victim"], in_DB["TEXT"]["cat"]["data_Victim"],

					not_in_DB["TEXT"]["num"], not_in_DB["TEXT"]["data"], 
					not_in_DB["TEXT"]["cat"]["Food"], not_in_DB["TEXT"]["cat"]["data_Food"],
					not_in_DB["TEXT"]["cat"]["Health"], not_in_DB["TEXT"]["cat"]["data_Health"],
					not_in_DB["TEXT"]["cat"]["Shelter"], not_in_DB["TEXT"]["cat"]["data_Shelter"],
					not_in_DB["TEXT"]["cat"]["SocialShare"], not_in_DB["TEXT"]["cat"]["data_SocialShare"],
					not_in_DB["TEXT"]["cat"]["Victim"], not_in_DB["TEXT"]["cat"]["data_Victim"],

					in_DB["TEXT"]["num"]/ (in_DB["TEXT"]["num"] + not_in_DB["TEXT"]["num"]),
					in_DB["TEXT"]["latency"]/in_DB["TEXT"]["num"] if (in_DB["TEXT"]["num"]!=0) else 0,
					in_DB["TEXT"]["std_deviation"] ])

	writer.writerow(['RECORDING', in_DB["RECORDING"]["num"], in_DB["RECORDING"]["data"], 
					in_DB["RECORDING"]["cat"]["Food"], in_DB["RECORDING"]["cat"]["data_Food"],
					in_DB["RECORDING"]["cat"]["Health"], in_DB["RECORDING"]["cat"]["data_Health"],
					in_DB["RECORDING"]["cat"]["Shelter"], in_DB["RECORDING"]["cat"]["data_Shelter"],
					in_DB["RECORDING"]["cat"]["SocialShare"], in_DB["RECORDING"]["cat"]["data_SocialShare"],
					in_DB["RECORDING"]["cat"]["Victim"], in_DB["RECORDING"]["cat"]["data_Victim"],

					not_in_DB["RECORDING"]["num"], not_in_DB["RECORDING"]["data"], 
					not_in_DB["RECORDING"]["cat"]["Food"], not_in_DB["RECORDING"]["cat"]["data_Food"],
					not_in_DB["RECORDING"]["cat"]["Health"], not_in_DB["RECORDING"]["cat"]["data_Health"],
					not_in_DB["RECORDING"]["cat"]["Shelter"], not_in_DB["RECORDING"]["cat"]["data_Shelter"],
					not_in_DB["RECORDING"]["cat"]["SocialShare"], not_in_DB["RECORDING"]["cat"]["data_SocialShare"],
					not_in_DB["RECORDING"]["cat"]["Victim"], not_in_DB["RECORDING"]["cat"]["data_Victim"],

					in_DB["RECORDING"]["num"]/ (in_DB["RECORDING"]["num"] + not_in_DB["RECORDING"]["num"]),
					in_DB["RECORDING"]["latency"]/in_DB["RECORDING"]["num"] if (in_DB["RECORDING"]["num"]!=0) else 0,
					in_DB["RECORDING"]["std_deviation"] ])

with open('dist.csv', 'wb') as csv_file:
	writer = csv.writer(csv_file)
	for i in range(0, len(dist_in_DB)):
		writer.writerow([i, dist_in_DB[i]["num"], dist_in_DB[i]["data"], 
			dist_in_DB[i]["cat"]["Food"], dist_in_DB[i]["cat"]["data_Food"], 
			dist_in_DB[i]["cat"]["Health"], dist_in_DB[i]["cat"]["data_Health"],
			dist_in_DB[i]["cat"]["Shelter"], dist_in_DB[i]["cat"]["data_Shelter"],
			dist_in_DB[i]["cat"]["SocialShare"], dist_in_DB[i]["cat"]["data_SocialShare"],
			dist_in_DB[i]["cat"]["Victim"], dist_in_DB[i]["cat"]["data_Victim"],
			dist_in_DB[i]["num"]/(dist_in_DB[i]["num"] + dist_not_in_DB[i]["num"]) if (dist_in_DB[i]["num"] + dist_not_in_DB[i]["num"] != 0) else 0,
			dist_in_DB[i]["latency"]/dist_in_DB[i]["num"] if (dist_in_DB[i]["num"]!=0) else 0
			])

with open('dist_not.csv', 'wb') as csv_file:
	writer = csv.writer(csv_file)
	for i in range(0, len(dist_not_in_DB)):
		writer.writerow([i, dist_not_in_DB[i]["num"], dist_not_in_DB[i]["data"], 
			dist_not_in_DB[i]["cat"]["Food"], dist_not_in_DB[i]["cat"]["data_Food"], 
			dist_not_in_DB[i]["cat"]["Health"], dist_not_in_DB[i]["cat"]["data_Health"],
			dist_not_in_DB[i]["cat"]["Shelter"], dist_not_in_DB[i]["cat"]["data_Shelter"],
			dist_not_in_DB[i]["cat"]["SocialShare"], dist_not_in_DB[i]["cat"]["data_SocialShare"],
			dist_not_in_DB[i]["cat"]["Victim"], dist_not_in_DB[i]["cat"]["data_Victim"]
			])

time_required["total"] = sorted(time_required["total"])
time_required["SMS"] = sorted(time_required["SMS"])
time_required["VIDEO"] = sorted(time_required["VIDEO"])
time_required["IMAGE"] = sorted(time_required["IMAGE"])
time_required["TEXT"] = sorted(time_required["TEXT"])
time_required["RECORDING"] = sorted(time_required["RECORDING"])
# print json.dumps(time_required,sort_keys=False,indent=4)


# print time_required
total_files = in_DB["count"]["num"] + not_in_DB["count"]["num"] 
delivery_wrt_time = []
for i in range(0,len(time_required["total"])):
	delivery_wrt_time.insert(i,(i+1.0)/total_files * 100.0)
# print delivery_wrt_time

with open('total.csv', 'wb') as csv_file:
	writer = csv.writer(csv_file)
	for i in range(0,len(time_required["total"])):
		writer.writerow([time_required["total"][i],delivery_wrt_time[i]])

total_files = in_DB["SMS"]["num"] + not_in_DB["SMS"]["num"] 
delivery_wrt_time = []
for i in range(0,len(time_required["SMS"])):
	delivery_wrt_time.insert(i,(i+1.0)/total_files * 100.0)
# print delivery_wrt_time

with open('sms.csv', 'wb') as csv_file:
	writer = csv.writer(csv_file)
	for i in range(0,len(time_required["SMS"])):
		writer.writerow([time_required["SMS"][i],delivery_wrt_time[i]])

total_files = in_DB["VIDEO"]["num"] + not_in_DB["VIDEO"]["num"] 
delivery_wrt_time = []
for i in range(0,len(time_required["VIDEO"])):
	delivery_wrt_time.insert(i,(i+1.0)/total_files * 100.0)
# print delivery_wrt_time

with open('video.csv', 'wb') as csv_file:
	writer = csv.writer(csv_file)
	for i in range(0,len(time_required["VIDEO"])):
		writer.writerow([time_required["VIDEO"][i],delivery_wrt_time[i]])

total_files = in_DB["IMAGE"]["num"] + not_in_DB["IMAGE"]["num"] 
delivery_wrt_time = []
for i in range(0,len(time_required["IMAGE"])):
	delivery_wrt_time.insert(i,(i+1.0)/total_files * 100.0)
# print delivery_wrt_time

with open('image.csv', 'wb') as csv_file:
	writer = csv.writer(csv_file)
	for i in range(0,len(time_required["IMAGE"])):
		writer.writerow([time_required["IMAGE"][i],delivery_wrt_time[i]])

total_files = in_DB["TEXT"]["num"] + not_in_DB["TEXT"]["num"] 
delivery_wrt_time = []
for i in range(0,len(time_required["TEXT"])):
	delivery_wrt_time.insert(i,(i+1.0)/total_files * 100.0)
# print delivery_wrt_time

with open('text.csv', 'wb') as csv_file:
	writer = csv.writer(csv_file)
	for i in range(0,len(time_required["TEXT"])):
		writer.writerow([time_required["TEXT"][i],delivery_wrt_time[i]])

total_files = in_DB["RECORDING"]["num"] + not_in_DB["RECORDING"]["num"] 
delivery_wrt_time = []
for i in range(0,len(time_required["RECORDING"])):
	delivery_wrt_time.insert(i,(i+1.0)/total_files * 100.0)
# print delivery_wrt_time

with open('recording.csv', 'wb') as csv_file:
	writer = csv.writer(csv_file)
	for i in range(0,len(time_required["RECORDING"])):
		writer.writerow([time_required["RECORDING"][i],delivery_wrt_time[i]])

