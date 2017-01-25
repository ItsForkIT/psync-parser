# Script to analyse all psync log files and store file info in db

import pymongo as pm
import os
import numpy as np 
import datetime
import time

try:
	connection = pm.MongoClient()
	db = connection.disarm_technoshine # switch to disarm database
except:
	print "Cannot connect to the mongo client. Please check the port address"

# Function to analyse a single file
# @param: file_path: path to the file
# @param: path_to_name: the names of the nodes corresponding to the file paths
def analyse_file(file_path, path_to_name):
	data = [np.array(map(None, line.split(','))) for line in open(file_path)]

	index = file_path.rfind('/')
	path = file_path[:index]
	p = path_to_name[path]

	time=[]
	msgType=[]
	time.append(0)
	msgType.append(data[0][1])
	for i in range(1,len(data)):
		start = datetime.datetime.strptime(data[0][0], '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
		end = datetime.datetime.strptime(data[i][0], '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
		delta = end - start
		time.append (delta.total_seconds())
		msgType.append(data[i][1])

	map_id_dbData = {} 				# map file id to data to store in db
	map_id_name = {} 				# map file id to file name
	map_id_str={} 					# map file id to start file download time
	map_id_end={} 					# map file id to stop file download time
	map_id_starting_byte={} 		# map file id to starting byte 
	map_id_ending_byte={}			# map file id to ending byte
	for i in range(0,len(data)):
		if(msgType[i] == ' START_FILE_DOWNLOAD'):
			map_id_str[data[i][2]] = time[i]
			map_id_starting_byte[data[i][2]]=int(data[i][4])
			map_id_name[data[i][2]] = data[i][3]
			node = {}
			node[p + "_psyncLog"] = {}
			node[p + "_psyncLog"]["timestamp_start_download"] = data[i][0]
			node[p + "_psyncLog"]["start_download"] = time[i]
			node[p + "_psyncLog"]["start_byte"] = int(data[i][4])
			map_id_dbData[data[i][2]] = node
		if(msgType[i] == ' STOP_FILE_DOWNLOAD'):
			map_id_end[data[i][2]] = time[i]
			map_id_ending_byte[data[i][2]]=int(data[i][4])
			node = map_id_dbData[data[i][2]]
			node[p + "_psyncLog"]["timestamp_stop_download"] = data[i][0]
			node[p + "_psyncLog"]["stop_download"] = time[i]
			node[p + "_psyncLog"]["time_taken"] = node[p + "_psyncLog"]["stop_download"] - node[p + "_psyncLog"]["start_download"]
			node[p + "_psyncLog"]["end_byte"] = int(data[i][4])
			map_id_dbData[data[i][2]] = node

	"""
	print map_id_str
	print "\n\n\n\n\n"
	print map_id_end
	"""
	max_time_taken=0
	size_max_time=0
	time_taken=[]
	tot_time_taken=0
	tot_data_transferred=0
	max_file_size=0
	time_max_file=0
	count1=0
	count2=0
	for keys in map_id_str:
		if(keys in map_id_end):
			count2=count2+1
			start=map_id_str[keys]
			end=map_id_end[keys]
			time_taken.append(end-start)
			tot_time_taken += (end-start)
		
			if(end-start>max_time_taken):
				max_time_taken=end-start
				size_max_time = map_id_ending_byte[keys]-map_id_starting_byte[keys]
		
			tot_data_transferred += (map_id_ending_byte[keys]-map_id_starting_byte[keys])

			if(max_file_size < (map_id_ending_byte[keys]-map_id_starting_byte[keys])):
				max_file_size = (map_id_ending_byte[keys]-map_id_starting_byte[keys])
				time_max_file = (end-start)
		else:
			count1=count1+1
	
	#print time_taken
	#print tot_time_taken
	try:
		if(len(time_taken)>0):
			print 'Average time for downloading a file is ', tot_time_taken/len(time_taken), ' seconds measured over ', len(time_taken), ' downloads '
		else:
			print 'Time taken = 0 .... No Downloads??'
		print 'Maximum time for a download is ', max_time_taken, ' seconds for a file of size ', size_max_time, ' bytes'
		print 'Maximum size of a single file transferred is ', max_file_size, ' bytes time taken is ', time_max_file, ' seconds'
		print 'Total data transferred ', tot_data_transferred, ' bytes '
		if(tot_time_taken>0):
			print 'Speed ', tot_data_transferred/tot_time_taken, ' bytes per second'
		else:
			print 'Speed inconclusive, total time taken = 0'

		print 'Number of failed transfers ', count1
		print 'Number of successful transfers ', count2
	except:
		print "Exception caught"

	#print map_id_dbData
	for keys in map_id_dbData:
		node = {}
		node = map_id_dbData[keys] # get file information
		# print node
		# insert node in corresponding file table in db
		name = map_id_name[keys] # get file name
		name = name.strip()
		cursor = db.files.find({"NAME":name})
		if(cursor.count()<=0):
			print "NOT FOUND",name
		else:
			try:
				new_node = {}
				new_node = cursor[0]
				new_node[p]["timestamp_start_download"] = node[p + "_psyncLog"]["timestamp_start_download"]
				new_node[p]["start_download"] = node[p + "_psyncLog"]["start_download"]
				new_node[p]["start_byte"] = node[p + "_psyncLog"]["start_byte"]
				new_node[p]["timestamp_stop_download"] = node[p + "_psyncLog"]["timestamp_stop_download"]
				new_node[p]["stop_download"] = node[p + "_psyncLog"]["stop_download"]
				new_node[p]["time_taken"] = node[p + "_psyncLog"]["time_taken"]
				new_node[p]["end_byte"] = node[p + "_psyncLog"]["end_byte"]
				# db.files.update_one({"NAME":name},{"$set":node})
				print new_node
				db.files.replace_one({"NAME":name},new_node)
				print "Succesful Updation"
			except:
				print "ERROR ==== CHECK "


paths = ["/home/arka/Desktop/Dumps/ex/7031583420",
		"/home/arka/Desktop/Dumps/ex/7076302126",
		"/home/arka/Desktop/Dumps/ex/7076525081",
		"/home/arka/Desktop/Dumps/ex/7076884054",
		"/home/arka/Desktop/Dumps/ex/7098642908",
		"/home/arka/Desktop/Dumps/ex/7477666542",
		"/home/arka/Desktop/Dumps/ex/7478147148",
		"/home/arka/Desktop/Dumps/ex/7551047169",
		"/home/arka/Desktop/Dumps/ex/7739924781",
		"/home/arka/Desktop/Dumps/ex/7785845447",
		"/home/arka/Desktop/Dumps/ex/8220309989",
		"/home/arka/Desktop/Dumps/ex/8370840532",
		"/home/arka/Desktop/Dumps/ex/8423217878",
		"/home/arka/Desktop/Dumps/ex/8436143487",
		"/home/arka/Desktop/Dumps/ex/8436145897",
		"/home/arka/Desktop/Dumps/ex/8537807840",
		"/home/arka/Desktop/Dumps/ex/8808020941",
		"/home/arka/Desktop/Dumps/ex/8874705428",
		"/home/arka/Desktop/Dumps/ex/8879716459",
		"/home/arka/Desktop/Dumps/ex/9433075181",
		"/home/arka/Desktop/Dumps/ex/9434820751",
		"/home/arka/Desktop/Dumps/ex/9534490018",
		"/home/arka/Desktop/Dumps/ex/9563971757",
		"/home/arka/Desktop/Dumps/ex/9563972030",
		"/home/arka/Desktop/Dumps/ex/9598453635",
		"/home/arka/Desktop/Dumps/ex/9635460583",
		"/home/arka/Desktop/Dumps/ex/9674967768",
		"/home/arka/Desktop/Dumps/ex/9713474997",
		"/home/arka/Desktop/Dumps/ex/9830505305",
		"/home/arka/Desktop/Dumps/ex/9851271032",
		"/home/arka/Desktop/Dumps/ex/9977991907"]

# map the names of the nodes corresponding to the file paths
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

for path in paths:
	count = 0;
	# get better way to get files
	for root, dirs, files in os.walk(path):
		for file in files:
			if(file.startswith("psyncLog")):
				count = count + 1
				print "================================================================================================"
				print "-----------------------Analysing ", path, "/", file, "--------------------------------"
				analyse_file(path + "/" + file, path_to_name)
	print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXFound ", count, " log files in ", path, "XXXXXXXXXXXXXXXXXXXXXXXXXX"