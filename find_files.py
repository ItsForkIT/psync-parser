# Script to find all unique files in our network
# Store all unique file info in mongoDb
# input : the sync/Working directory of nodes

import pymongo as pm 
import os
import time
import csv

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
	index = path.rfind('/')
	new_path = path[:index]
	print new_path
	text_file = open(new_path + "/source.txt","r")
	path_to_name[path] = text_file.read()
	print path_to_name
#print path_to_name

"""
Find all unique files in our network
For every unique file keep track of the name, type, date of creation, source node of file
Also keep details of that file present in every node
"""
for path in paths:
	for root, dirs, files in os.walk(path):
		for file in files:
			if file.endswith(".txt") or file.endswith(".3gp") or file.endswith(".jpg"):
				# get all file info and put it in a db
				cursor = db.files.find({"NAME":file})
				if(cursor.count()>0):
					print "Updating for node ", path
					
					fileinfo = cursor[0]
					p = path_to_name[path]
					associated_nodes = fileinfo["ASSOCIATED_NODES"]
					associated_nodes.append(p)
					statinfo = os.stat(path + "/" + file)
					node = {}
					node["ASSOCIATED_NODES"] = associated_nodes
					node[p] = {}
					node[p]["size"] = statinfo.st_size
					node[p]["date_created"] = time.ctime(statinfo.st_ctime)
					node[p]["date_modified"] = time.ctime(statinfo.st_mtime)
					db.files.update_one({"NAME":cursor[0]["NAME"]},{"$set":node})
				else:
					statinfo = os.stat(path + "/" + file)
					fileinfo = {}
					file_name_split = file.split('_')
					fileinfo["NAME"] = file
					if(file.startswith("Map")):
						# no destination for Map logs
						fileinfo["SOURCE"] = file_name_split[2] 
					else:
						fileinfo["SOURCE"] = file_name_split[3]
						fileinfo["DESTINATION"] = file_name_split[4]
					
					if(file.startswith("Map")):
						# no creation data for Map logs
						fileinfo["DATE_OF_CREATION"] = file_name_split[3].split('.')[0]
						fileinfo["MSG_TYPE"] = 'MAP_LOG'
					else:
						fileinfo["DATE_OF_CREATION"] = file_name_split[7]
						fileinfo["CREATION_LAT"] = file_name_split[5]
						fileinfo["CREATION_LON"] = file_name_split[6]
						fileinfo["MSG_TYPE"] = file_name_split[2]
					
					p = path_to_name[path]
					associated_nodes = []
					associated_nodes.append(p)
					fileinfo["ASSOCIATED_NODES"] = associated_nodes
					fileinfo[p] = {}
					fileinfo[p]["size"] = statinfo.st_size
					fileinfo[p]["date_created"] = time.ctime(statinfo.st_ctime)
					fileinfo[p]["date_modified"] = time.ctime(statinfo.st_mtime)
					if file.startswith("SMS"):
						fileinfo["type"] = "SMS"
					elif file.startswith("VID"):
						fileinfo["type"] = "VIDEO"
					elif file.startswith("IMG"):
						fileinfo["type"] = "IMAGE"
					elif file.startswith("TXT"):
						fileinfo["type"] = "TEXT"
					elif file.startswith("SVS"):
						fileinfo["type"] = "RECORDING"
					elif file.startswith("MapDisarm"):
						fileinfo["type"] = "GPS TRAIL"
					
					db.files.insert_one(fileinfo)
					print "File inserted in db"
