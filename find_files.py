# Script to find all unique files in our network
# Store all unique file info in mongoDb
# input : the sync/Working directory of nodes

import pymongo as pm 
import os
import time
import csv

try:
	connection = pm.MongoClient()
	db = connection.mobicom # switch to mobicom database
except:
	print "Cannot connect to the mongo client. Please check the port address"


"""
Add all Working Directory in paths
"""
paths = []
for root, dirs, files in os.walk('./Dump/'):
	for name in dirs:
		path = os.path.join(root, name)
		# print path
		if path.endswith('Working') or path.endswith('sync'):
			paths.append(path)


"""
Keep a map of the working directory of each node to its node name
"""
path_to_name = {}
for path in paths:
	index = path.rfind('/')
	new_path = path[:index]
	index = new_path.rfind('/')
	new_path = new_path[index + 1:]	
	path_to_name[path] = new_path
	# print path_to_name
#print path_to_name

"""
Find all unique files in our network
For every unique file keep track of the name, type, date of creation, source node of file
Also keep details of that file present in every node
"""
for path in paths:
	for root, dirs, files in os.walk(path):
		for file in files:
			if file.startswith("IMG") or file.startswith("VID") or file.startswith("SMS") or file.startswith("TXT") or file.startswith("SVG"):
				# get all file info and put it in a db
				cursor = db.files.find({"NAME":file})
				if(cursor.count()>0):
					# print "Updating for node ", path
					
					fileinfo = cursor[0]
					p = path_to_name[path]
					associated_nodes = fileinfo["ASSOCIATED_NODES"]
					associated_nodes.append(p)
					statinfo = os.stat(path + "/" + file)
					node = {}
					node["ASSOCIATED_NODES"] = associated_nodes
					node[p] = {}
					node[p]["size"] = statinfo.st_size
					# node[p]["date_created"] = time.ctime(statinfo.st_ctime)
					# node[p]["date_modified"] = time.ctime(statinfo.st_mtime)
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
					# fileinfo[p]["date_created"] = time.ctime(statinfo.st_ctime)
					# fileinfo[p]["date_modified"] = time.ctime(statinfo.st_mtime)
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
					# print "File inserted in db"
