# Analyse inter-cluster data transfers

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

try:
	connection = pm.MongoClient()
	db = connection.mobicom # switch to mobicom database
except:
	print "Cannot connect to the mongo client. Please check the port address"

def updateBoxResults(source, box, cluster, document):
	"""
	Method to update results from a cluster to a Box( DB or MCS)
	"""

	cluster[box][document["MSG_TYPE"] + "_count"] += 1
	cluster[box]["data_" + document["MSG_TYPE"]] += document[source]["size"]

	# Update by file type
	name = document["NAME"]
	name = str(name)
	if name.startswith("SMS"):
		cluster[box]["Sms_count"] += 1
		cluster[box]["data_Sms"] += document[source]["size"]
	elif name.startswith("VID"):
		cluster[box]["Video_count"] += 1
		cluster[box]["data_Video"] += document[source]["size"]
	elif name.startswith("IMG"):
		cluster[box]["Image_count"] += 1
		cluster[box]["data_Image"] += document[source]["size"]
	elif name.startswith("TXT"):
		cluster[box]["Text_count"] += 1
		cluster[box]["data_Text"] += document[source]["size"]
	elif name.startswith("SVG"):
		cluster[box]["Recording_count"] += 1
		cluster[box]["data_Recording"] += document[source]["size"]


def updateCluster(source, cluster, document):
	"""
	Update cluster wise
	"""
	# Update by msg type
	cluster[document["MSG_TYPE"] + "_count"] = cluster[document["MSG_TYPE"] + "_count"] + 1
	cluster["data_" + document["MSG_TYPE"]] = cluster["data_" + document["MSG_TYPE"]] + document[source]["size"]

	# Update by file type
	name = document["NAME"]
	name = str(name)
	if name.startswith("SMS"):
		cluster["Sms_count"] = cluster["Sms_count"] + 1
		cluster["data_Sms"] = cluster["data_Sms"] + document[source]["size"]
	elif name.startswith("VID"):
		cluster["Video_count"] = cluster["Video_count"] + 1
		cluster["data_Video"] = cluster["data_Video"] + document[source]["size"]
	elif name.startswith("IMG"):
		cluster["Image_count"] = cluster["Image_count"] + 1
		cluster["data_Image"] = cluster["data_Image"] + document[source]["size"]
	elif name.startswith("TXT"):
		cluster["Text_count"] = cluster["Text_count"] + 1
		cluster["data_Text"] = cluster["data_Text"] + document[source]["size"]
	elif name.startswith("SVG"):
		cluster["Recording_count"] = cluster["Recording_count"] + 1
		cluster["data_Recording"] = cluster["data_Recording"] + document[source]["size"]


cursor = db.files.find()

cluster1 = ["9474649114", "9888844036", "9836062742", "DB1"]
cluster2 = ["9434789009", "9635547701", "9475610485", "DB2"]
cluster3 = ["8135002680", "9435304558", "7909030931", "defaultMcs"]

info = { 	# data will indicate total data transfered, irrespective of complete or partial transfer
			# count will indicate only complete transfers
	"Food_count": 0,
	"Victim_count": 0,
	"Shelter_count": 0,
	"Health_count": 0,
	"SocialShare_count": 0,
	
	"data_Food": 0.0,
	"data_Health": 0.0,
	"data_Shelter": 0.0,
	"data_SocialShare": 0.0,
	"data_Victim": 0.0,

	"Sms_count": 0,
	"Video_count": 0,
	"Image_count": 0,
	"Text_count": 0,
	"Recording_count": 0,

	"data_Sms": 0.0,
	"data_Video": 0.0,
	"data_Image": 0.0,
	"data_Text": 0.0,
	"data_Recording": 0.0
}

c_info = copy.deepcopy(info)
c_info["DB1"] = copy.deepcopy(info)
c_info["DB2"] = copy.deepcopy(info)
c_info["defaultMcs"] = copy.deepcopy(info)

cl1 = copy.deepcopy(c_info)
cl2 = copy.deepcopy(c_info)
cl3 = copy.deepcopy(c_info)

db1 = copy.deepcopy(info)
db2 = copy.deepcopy(info)
mcs = copy.deepcopy(info)

for document in cursor:
	# print document
	if document["type"] == "GPS TRAIL": # ignore GPS trails 
		continue
	if document["SOURCE"] not in document:
		continue

	source = document["SOURCE"]
	if source in cluster1:
		cl = cl1
		updateCluster(source, cl1, document)
	elif source in cluster2:
		cl = cl2
		updateCluster(source, cl2, document)
	elif source in cluster3:
		cl = cl3
		updateCluster(source, cl3, document)

	
	for node in document["ASSOCIATED_NODES"]:
		if document["SOURCE"] == node:
			continue

		if node == "DB1":
			updateBoxResults(source, "DB1", cl, document)
		elif node == "DB2":
			updateBoxResults(source, "DB2", cl, document)
		elif node == "defaultMcs":
			updateBoxResults(source, "defaultMcs", cl, document)
	
print json.dumps(cl1, indent=4, sort_keys=True)
print json.dumps(cl2, indent=4, sort_keys=True)
print json.dumps(cl3, indent=4, sort_keys=True)

with open('Results/cluster_analysis.csv', 'wb') as csv_file:
	writer = csv.writer(csv_file)
	writer.writerow([
		'Cluster', 'Db1:Food', 'Db1:Victim', 'Db1:Shelter', 'Db1:Health',
		'Db1:Food Data', 'Db1:Victim Data', 'Db1:Shelter Data', 'Db1:Health Data',
		'Db1:SMS', 'Db1:Video', 'Db1:Image', 'Db1:Text', 'Db1:Recording', 
		'Db1:SMS Data', 'Db1:Video Data', 'Db1:Image Data', 'Db1:Text Data', 'Db1:Recording Data',

		'Db2:Food', 'Db2:Victim', 'Db2:Shelter', 'Db2:Health',
		'Db2:Food Data', 'Db2:Victim Data', 'Db2:Shelter Data', 'Db2:Health Data',
		'Db2:SMS', 'Db2:Video', 'Db2:Image', 'Db2:Text', 'Db2:Recording', 
		'Db2:SMS Data', 'Db2:Video Data', 'Db2:Image Data', 'Db2:Text Data', 'Db2:Recording Data', 

		'MCS:Food', 'MCS:Victim', 'MCS:Shelter', 'MCS:Health',
		'MCS:Food Data', 'MCS:Victim Data', 'MCS:Shelter Data', 'MCS:Health Data',
		'MCS:SMS', 'MCS:Video', 'MCS:Image', 'MCS:Text', 'MCS:Recording', 
		'MCS:SMS Data', 'MCS:Video Data', 'MCS:Image Data', 'MCS:Text Data', 'MCS:Recording Data', 

		'Cluster:Food', 'Cluster:Victim', 'Cluster:Shelter', 'Cluster:Health',
		'Cluster:Food Data', 'Cluster:Victim Data', 'Cluster:Shelter Data', 'Cluster:Health Data',
		'Cluster:SMS', 'Cluster:Video', 'Cluster:Image', 'Cluster:Text', 'Cluster:Recording', 
		'Cluster:SMS Data', 'Cluster:Video Data', 'Cluster:Image Data', 'Cluster:Text Data', 'Cluster:Recording Data'
		])

	for cl in [cl1, cl2, cl3]:
		row = ['Cluster 1']

		for DB in ['DB1', 'DB2', 'defaultMcs']:
			row = row + [
			cl[DB]['Food_count'], cl[DB]['Victim_count'], cl[DB]['Shelter_count'], cl[DB]['Health_count'], 
			cl[DB]['data_Food']/1000000.0, cl[DB]['data_Victim']/1000000.0, cl[DB]['data_Shelter']/1000000.0, cl[DB]['data_Health']/1000000.0, 
			cl[DB]['Sms_count'], cl[DB]['Video_count'], cl[DB]['Image_count'], cl[DB]['Text_count'], cl[DB]['Recording_count'],
			cl[DB]['data_Sms']/1000000.0, cl[DB]['data_Video']/1000000.0, cl[DB]['data_Image']/1000000.0, cl[DB]['data_Text']/1000000.0, cl[DB]['data_Recording']/1000000.0
			]

		row = row + [
			cl['Food_count'], cl['Victim_count'], cl['Shelter_count'], cl['Health_count'], 
			cl['data_Food']/1000000.0, cl['data_Victim']/1000000.0, cl['data_Shelter']/1000000.0, cl['data_Health']/1000000.0, 
			cl['Sms_count'], cl['Video_count'], cl['Image_count'], cl['Text_count'], cl['Recording_count'],
			cl['data_Sms']/1000000.0, cl['data_Video']/1000000.0, cl['data_Image']/1000000.0, cl['data_Text']/1000000.0, cl['data_Recording']/1000000.0
			]

		writer.writerow(row)