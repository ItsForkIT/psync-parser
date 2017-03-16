# Script to write relevant file infos into a file from database

import pymongo as pm 
import os
import time
import csv

try:
	connection = pm.MongoClient()
	db = connection.mobicom # switch to mobicom database
except:
	print "Cannot connect to the mongo client. Please check the port address"


cursor = db.files.find()
allFiles = []
#count = db.files.count()
count = 0
for document in cursor:
	# print document
	if document["type"] == "GPS TRAIL": # ignore GPS trails 
		continue
	if document["SOURCE"] not in document:
		continue

	allFiles.append({})
	allFiles[count]["NAME"] = document["NAME"]
	allFiles[count]["TYPE"] = document["type"]
	allFiles[count]["MSG_TYPE"] = document["MSG_TYPE"]
	allFiles[count]["SOURCE"] = document["SOURCE"]
	allFiles[count]["DATE_OF_CREATION"] = document["DATE_OF_CREATION"]
	allFiles[count]["CREATION_LAT"] = document["CREATION_LAT"]
	allFiles[count]["CREATION_LON"] = document["CREATION_LON"]
	allFiles[count]["DESTINATION"] = document["DESTINATION"]
	associated_nodes = document["ASSOCIATED_NODES"]

	if document["SOURCE"] in document:
		allFiles[count]["SIZE"] = document[document["SOURCE"]]["size"]
		associated_nodes.remove(document["SOURCE"])
	else:
		allFiles[count]["SIZE"] = -99

	if (document["DESTINATION"] in document and document[document["DESTINATION"]]["size"] == allFiles[count]["SIZE"] ):
		allFiles[count]["DEST_REACHED"] = 'YES'
		allFiles[count]["DEST_SIZE"] = document[document["DESTINATION"]]["size"]
		allFiles[count]["DEST_REACH_TIME"] = document[document["DESTINATION"]]["timestamp_stop_download"]
		if ("LAT_stop_download" in document[document["DESTINATION"]] ):
			allFiles[count]["DEST_LAT"] = document[document["DESTINATION"]]["LAT_stop_download"]
			allFiles[count]["DEST_LON"] = document[document["DESTINATION"]]["LON_stop_download"]
		else:
			allFiles[count]["DEST_LAT"] = "Not Available"
			allFiles[count]["DEST_LON"] = "Not Available"
		associated_nodes.remove(document["DESTINATION"])

	elif (document["DESTINATION"] in document and document[document["DESTINATION"]]["size"] != allFiles[count]["SIZE"]):
		allFiles[count]["DEST_REACHED"] = 'PARTIAL'
		allFiles[count]["DEST_SIZE"] = document[document["DESTINATION"]]["size"]
		allFiles[count]["DEST_REACH_TIME"] = document[document["DESTINATION"]]["timestamp_stop_download"]
		if ("LAT_stop_download" in document[document["DESTINATION"]] ):
			allFiles[count]["DEST_LAT"] = document[document["DESTINATION"]]["LAT_stop_download"]
			allFiles[count]["DEST_LON"] = document[document["DESTINATION"]]["LON_stop_download"]
		else:
			allFiles[count]["DEST_LAT"] = "Not Available"
			allFiles[count]["DEST_LON"] = "Not Available"
		associated_nodes.remove(document["DESTINATION"])

	else:
		allFiles[count]["DEST_REACHED"] = 'NO'
		allFiles[count]["DEST_SIZE"] = -99
		allFiles[count]["DEST_REACH_TIME"] = "----.--.--.--.--.--"
		allFiles[count]["DEST_LAT"] = "Not Available"
		allFiles[count]["DEST_LON"] = "Not Available"

	allFiles[count]["ASSOCIATED_NODES"] = associated_nodes

	# Calc total sizes of all copies present with associated nodes
	file_overhead = 0
	for node in associated_nodes:
		file_overhead = file_overhead + document[node]["size"]

	allFiles[count]["DATA_OVERHEAD"] = file_overhead/1000000.0 # in MB
	count = count + 1

with open('Results/files.csv', 'wb') as csv_file:
	writer = csv.writer(csv_file)
	writer.writerow(['Name', 'Type', 'Msg Type', 'Source', 'Creation Date', 'Creation Time', 'Latitude', 
		'Longitude', 'Size', 'Destination Reached Status', 'Destination', 'Intermediate Node Count', 'Intermediate Nodes',
		'Size at Destination', 'Destination Reach Date', 'Destination Reach Time', 'Destination Latitude', 
		'Destination Longitude', 'Data Overhead'])
	for i in range(0, len(allFiles)):
		writer.writerow( [ allFiles[i]["NAME"], allFiles[i]["TYPE"], allFiles[i]["MSG_TYPE"], allFiles[i]["SOURCE"], 
			allFiles[i]["DATE_OF_CREATION"][0:4] + '.' + allFiles[i]["DATE_OF_CREATION"][4:6] + '.' + allFiles[i]["DATE_OF_CREATION"][6:8],
			allFiles[i]["DATE_OF_CREATION"][8:10] + '.' + allFiles[i]["DATE_OF_CREATION"][10:12] + '.' + allFiles[i]["DATE_OF_CREATION"][12:],
			allFiles[i]["CREATION_LAT"], allFiles[i]["CREATION_LON"], allFiles[i]["SIZE"]/1000000.0, allFiles[i]["DEST_REACHED"],
			allFiles[i]["DESTINATION"], len(allFiles[i]["ASSOCIATED_NODES"]), allFiles[i]["ASSOCIATED_NODES"],
			allFiles[i]["DEST_SIZE"]/1000000.0, allFiles[i]["DEST_REACH_TIME"][0:10], allFiles[i]["DEST_REACH_TIME"][11:], allFiles[i]["DEST_LAT"],
			allFiles[i]["DEST_LON"], allFiles[i]["DATA_OVERHEAD"] ] )
