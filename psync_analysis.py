# Script to analyse all psync log files and store file info in db

import pymongo as pm
import os
import numpy as np 
import datetime
import time
import copy
import json

try:
	connection = pm.MongoClient()
	db = connection.kgecflighttest 
except:
	print "Cannot connect to the mongo client. Please check the port address"

# Function to analyse a single file
# @param: file_path: path to the file
# @param: path_to_name: the names of the nodes corresponding to the file paths
def analyse_file(file_path, db_psync_path, count):
	offset = 19800
	data = [np.array(map(None, line.split(','))) for line in open(file_path)]

	start = datetime.datetime.strptime(data[0][0], '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
	end = datetime.datetime.strptime(data[len(data)-1][0], '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
	session_time = (end - start).total_seconds()

	data = [np.array(map(None, line.split(','))) for line in open(db_psync_path)]
	
	in_session = False
	peer_connected = False
	f = {
	"start_download_byte" : 0.0,
	"end_download_byte" : 0.0,
	"start_download_row" : 0,
	"end_download_row" : 0 
	}
	cp = {
	"num": 0
	}
	downloads = {}

	total_data = 0.0
	for i in range(0, len(data)):

		timestamp = datetime.datetime.strptime(data[i][0], '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
		timestamp = timestamp + datetime.timedelta(hours=5,minutes=30)
		if timestamp >= start and timestamp <= end:
			in_session = True
		else:
			in_session = False

		if data[i][1] == " PEER_DISCOVERED" :
			peer_connected = True
		elif data[i][1] == " PEER_LOST":
			peer_connected = False

		elif "START_FILE_DOWNLOAD" in data[i][1] and in_session == True:
			name = data[i][3].strip(' ') # the file name
			if name not in downloads:
				downloads[name] = copy.deepcopy(cp)

			downloads[name]["num"] = downloads[name]["num"] + 1
			n = downloads[name]["num"]
			downloads[name][n] = copy.deepcopy(f)	
			downloads[name][n]["start_download_byte"] = float(data[i][4])
			downloads[name][n]["start_download_row"] = i 
		
		elif "STOP_FILE_DOWNLOAD" in data[i][1] and ( in_session == True):
			name = data[i][3].strip(' ')
			if name not in downloads : 
				for j in range(i, 0, -1) :
					if "START_FILE_DOWNLOAD" in data[j][1] and data[j][3].strip(' ') == name:
						downloads[name] = copy.deepcopy(cp)
						break

				downloads[name]["num"] = downloads[name]["num"] + 1
				n = downloads[name]["num"]
				downloads[name][n] = copy.deepcopy(f)	
				downloads[name][n]["start_download_byte"] = float(data[j][4])
				downloads[name][n]["start_download_row"] = j 

			n = downloads[name]["num"]
			downloads[name][n]["end_download_byte"] = float(data[i][4])
			downloads[name][n]["end_download_row"] = i
			data_f = (downloads[name][n]["end_download_byte"] - downloads[name][n]["start_download_byte"])
			#print "Adding ", downloads[name][n]["end_download_byte"],  " - ", downloads[name][n]["start_download_byte"]
			total_data = total_data + data_f

	#print json.dumps(downloads,sort_keys=False,indent=4)

	print " FOR  SESSION ", count, " Start time = ", start, " End time = ", end, ", TOTAL DATA = ", total_data, " TOTAL SECONDS = ", session_time



db_psync_path = "./KGECFlightTest/psyncLog-MCS-2017.01.26.11.33.07.csv"
node_source = "./DMS/source.txt"
path = "./DMS"

count = 0
for root, dirs, files in os.walk(path):
	for file in files:
		if(file.startswith("psyncLog")):
			count = count + 1
			print "================================================================================================"
			print "-----------------------Analysing ", path, "/", file, "--------------------------------"
			analyse_file(path + "/" + file, db_psync_path, count)
print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXFound ", count, " log files in ", path, "XXXXXXXXXXXXXXXXXXXXXXXXXX"