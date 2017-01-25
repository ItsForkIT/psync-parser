# Script to collect GPS location for every start and stop download of every unique files in database
# Update entries in db 

import pymongo as pm 
import os
import time
import datetime
import numpy as np 

try:
	connection = pm.MongoClient()
	db = connection.disarm_technoshine # switch to disarm database
except:
	print "Cannot connect to the mongo client. Please check the port address"

paths = ["/home/arka/Desktop/Dumps/ex/7031583420/Working",
		"/home/arka/Desktop/Dumps/ex/7076302126/Working",
		"/home/arka/Desktop/Dumps/ex/7076525081/Working",
		"/home/arka/Desktop/Dumps/ex/7076884054/Working",
		"/home/arka/Desktop/Dumps/ex/7098642908/Working",
		"/home/arka/Desktop/Dumps/ex/7477666542/Working",
		"/home/arka/Desktop/Dumps/ex/7478147148/Working",
		"/home/arka/Desktop/Dumps/ex/7551047169/Working",
		"/home/arka/Desktop/Dumps/ex/7739924781/Working",
		"/home/arka/Desktop/Dumps/ex/7785845447/Working",
		"/home/arka/Desktop/Dumps/ex/8220309989/Working",
		"/home/arka/Desktop/Dumps/ex/8370840532/Working",
		"/home/arka/Desktop/Dumps/ex/8423217878/Working",
		"/home/arka/Desktop/Dumps/ex/8436143487/Working",
		"/home/arka/Desktop/Dumps/ex/8436145897/Working",
		"/home/arka/Desktop/Dumps/ex/8537807840/Working",
		"/home/arka/Desktop/Dumps/ex/8808020941/Working",
		"/home/arka/Desktop/Dumps/ex/8874705428/Working",
		"/home/arka/Desktop/Dumps/ex/8879716459/Working",
		"/home/arka/Desktop/Dumps/ex/9433075181/Working",
		"/home/arka/Desktop/Dumps/ex/9434820751/Working",
		"/home/arka/Desktop/Dumps/ex/9534490018/Working",
		"/home/arka/Desktop/Dumps/ex/9563971757/Working",
		"/home/arka/Desktop/Dumps/ex/9563972030/Working",
		"/home/arka/Desktop/Dumps/ex/9598453635/Working",
		"/home/arka/Desktop/Dumps/ex/9635460583/Working",
		"/home/arka/Desktop/Dumps/ex/9674967768/Working",
		"/home/arka/Desktop/Dumps/ex/9713474997/Working",
		"/home/arka/Desktop/Dumps/ex/9830505305/Working",
		"/home/arka/Desktop/Dumps/ex/9851271032/Working",
		"/home/arka/Desktop/Dumps/ex/9977991907/Working"
		]
"""
Keep a map of the working directory of each node to its node name
"""
path_to_name = {}
for path in paths:
	if path[:4]=="./DB":
		path_to_name[path] = "DB"
	elif path[:4]=="./DM":
		path_to_name[path] = "DM"
	else:
		index = path.rfind('/')
		new_path = path[:index]
		text_file = open(new_path + "/source.txt","r")
		path_to_name[path] = text_file.read()
#print path_to_name

def analyse_file( file_path, timestamp, node, pass_no):
	data = [np.array(map(None, line.split(','))) for line in open(file_path)]

	if(pass_no == 2 and len(data) > 0):
		return data[0][0] + "_" + data[0][1]

	timestamp = datetime.datetime.strptime(timestamp, '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
	for i in range(0, len(data)):
		time = data[i][5]
		time = datetime.datetime.strptime(time, '%Y%m%d%H%M%S\r\n') + datetime.timedelta(days=1)

		if(time >= timestamp):
			return data[i][0] + "_" + data[i][1]

def findNodeLocation( timestamp, node, pass_no = 1):
	for path in paths:
		if path_to_name[path] == node:
			req_path = path
			break

	for root, dirs, files in os.walk(path):
		for file in files:
			if (file.startswith("Map") == False):
				continue

			file_name_split = file.split('_')
			if (file_name_split[2] != node):
				continue

			return analyse_file( path + "/" + file, timestamp, node, pass_no)

cursor = db.files.find()

for document in cursor:
	if document["type"] == "GPS TRAIL": # ignore GPS trails 
		continue

	file = document
	# find location at the time of creation of file
	# this bit is needed if location is not locked correctly
	if document["SOURCE"] in document:
		timestamp = document["DATE_OF_CREATION"]
		timestamp = timestamp[0:4]+'.'+timestamp[4:6]+'.'+timestamp[6:8]+'.'+timestamp[8:10]+'.'+timestamp[10:12]+'.'+timestamp[12:]
		loc = findNodeLocation(timestamp, document["SOURCE"])
		if ((loc is None) == False):
			file[document["SOURCE"]]["LAT_creation"] = loc.split('_')[0]
			file[document["SOURCE"]]["LON_creation"] = loc.split('_')[1]
		else:
			# For second pass we take the first location locked in trail
			# Since we do not get result in 1st pass the first data locked 
			# likely gives us the location of the node after the file was created
			loc = findNodeLocation(timestamp, document["SOURCE"], 2)
			if ((loc is None) == False):
				file[document["SOURCE"]]["LAT_creation"] = loc.split('_')[0]
				file[document["SOURCE"]]["LON_creation"] = loc.split('_')[1]
			else:
				print 'LOC NOT FOUND FOR SOURCE NODE ', document["SOURCE"], ' for file ',  document["NAME"], ' timestamp ', timestamp

	for node in document["ASSOCIATED_NODES"]:
		if "timestamp_start_download" in document[node]:
			loc = findNodeLocation( document[node]["timestamp_start_download"], node)
			if ((loc is None) == False):
				file[node]["LAT_start_download"] = loc.split('_')[0]
				file[node]["LON_start_download"] = loc.split('_')[1]
			else:
				print "loc not found for ", node, " for file ", document["NAME"], ' timestamp ', document[node]["timestamp_start_download"]

		if "timestamp_stop_download" in document[node]:
			loc = findNodeLocation( document[node]["timestamp_stop_download"], node)
			if ((loc is None) == False):
				file[node]["LAT_stop_download"] = loc.split('_')[0]
				file[node]["LON_stop_download"] = loc.split('_')[1]
			else:
				print "loc not found for ", node, " for file ", document["NAME"], ' timestamp ', document[node]["timestamp_start_download"]

	db.files.update_one({"NAME":document["NAME"]},{"$set":file})
	print "Updated for ", document["NAME"]