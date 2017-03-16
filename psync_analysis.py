# Script to analyse all psync log files and store file info in db

import pymongo as pm
import os
import numpy as np 
import datetime
import time
import copy
import csv
from math import sqrt

try:
	connection = pm.MongoClient()
	db = connection.mobicom # switch to mobicom database
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

	data_downloaded = 0.0
	time_total = 0.0
	delay = 0
	file_stack = {}
	peer_stack = {}
	connections = []
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

			if (data[i][3] not in file_stack) :
				file_stack[data[i][3]] = {}
				file_stack[data[i][3]]["byte"] = int(data[i][4])
				file_stack[data[i][3]]["time"] = data[i][0]

		if(msgType[i] == ' STOP_FILE_DOWNLOAD'):
			map_id_end[data[i][2]] = time[i]
			map_id_ending_byte[data[i][2]]=int(data[i][4])
			node = map_id_dbData[data[i][2]]
			node[p + "_psyncLog"]["timestamp_stop_download"] = data[i][0]
			node[p + "_psyncLog"]["stop_download"] = time[i]
			node[p + "_psyncLog"]["time_taken"] = node[p + "_psyncLog"]["stop_download"] - node[p + "_psyncLog"]["start_download"]
			node[p + "_psyncLog"]["end_byte"] = int(data[i][4])
			map_id_dbData[data[i][2]] = node

			data_downloaded += (int(data[i][4]) - file_stack[data[i][3]]["byte"] )

			connections_time = (datetime.datetime.strptime(data[i][0], '%Y.%m.%d.%H.%M.%S') - 
			datetime.datetime.strptime(file_stack[data[i][3]]["time"], '%Y.%m.%d.%H.%M.%S')).total_seconds()
			
			if connections_time > 0:
				connections.append( (int(data[i][4]) - file_stack[data[i][3]]["byte"])/connections_time )
			del(file_stack[ data[i][3] ])

			# calculate delay when file is downloaded completely
			# delay = time for stop download - time of file creation
			res = db.files.find_one({'NAME':data[i][3].strip()})
			if res != None and (int(data[i][4]) == res[res["SOURCE"]]["size"] ):
				file_creation_time = data[i][3].split('_')[7]
				delay += ( datetime.datetime.strptime(data[i][0], '%Y.%m.%d.%H.%M.%S') - 
							datetime.datetime.strptime(file_creation_time, '%Y%m%d%H%M%S') ).total_seconds()

		if (data[i][1] == ' PEER_DISCOVERED') :
			# print("+", data[i][2])
			peer_stack[data[i][2]] = data[i][0]
		elif (data[i][1] == ' PEER_LOST') :
			# print("-", data[i][2])
			try :
				time_total += ( datetime.datetime.strptime(data[i][0], '%Y.%m.%d.%H.%M.%S') - 
							datetime.datetime.strptime(peer_stack[data[i][2]], '%Y.%m.%d.%H.%M.%S') ).total_seconds()
				del(peer_stack[data[i][2]])
			except:
				pass

	print " Data downloaded is ", data_downloaded
	print " Total connection time ", time_total
	print " Total delay is ", delay

	# Find mean and standard deviation
	if len(connections) > 0 :
		mean = (sum(connections)*1.0)/len(connections) 
		print " Mean data downloaded / time over all connections ", mean
		differences = [x - mean for x in connections]
    	sq_differences = [d ** 2 for d in differences]
    	ssd = sum(sq_differences)
    	variance = ssd / len(connections)
    	sd = sqrt(variance)
    	print " Standard deviation over all connections ", sd

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
			# print "NOT FOUND",name
			name
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
				# print new_node
				db.files.replace_one({"NAME":name},new_node)
				# print "Succesful Updation"
			except:
				print "ERROR ==== CHECK  for ", name

	

def snapshot_working_dir(file_path, path_to_name):
	"""
	This method analysis psync log files and creates a 
	snapshot of the working dir for the node.
	A snapshot should contain all the informations
	required to calculate the importance of the 
	working dir of the node at that instant of time.
	"""

	index = file_path.rfind('/')
	path = file_path[:index]
	node = path_to_name[path]

	data = [np.array(map(None, line.split(','))) for line in open(file_path)]

	files_present = []	# will contain the list of files present in working dir
	files_completed = []# will contain the list of files completely downloaded from peer
	file = {}
	interval = {
	"number": 0,
	"start_timestamp": datetime.datetime.strptime(data[0][0], '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1),
	"end_timestamp": datetime.datetime.strptime(data[0][0], '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1),
	"file_count": 0, 	# total number of files present in this interval
	"file_delivered": 0,# total number of files completely delivered in this interval
	"data": 0.0, 		# data volume 
	"files": []
	}
	intervals = []
	interval_duration = 5 * 60 # 5 min interval time

	count = 0
	files_received = 0
	data_downloaded = 0.0
	interval_start_time = datetime.datetime.strptime(data[0][0], '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)
	for i in range(1, len(data)):
		time_now = datetime.datetime.strptime(data[i][0], '%Y.%m.%d.%H.%M.%S') + datetime.timedelta(days=1)

		if data[i][1] == ' START_FILE_DOWNLOAD':
			filename = data[i][3]
			if filename not in files_present:
				files_present.append(filename)
				file[filename] = data[i][4]

		elif data[i][1] == ' STOP_FILE_DOWNLOAD':
			filename = data[i][3]
			if int(data[i][4]) == int(float(data[i][5])):
				files_completed.append(filename)
			# update downloaded data 
			data_downloaded = data_downloaded + (int(data[i][4]) - int(file[filename]))
			file[filename] = data[i][4]

		if (time_now - interval_start_time).total_seconds() >= interval_duration:
			#update interval data here
			current_interval = copy.deepcopy(interval)
			current_interval["number"] = count + 1
			current_interval["file_count"] = len(files_present) # this denotes even partial downloads
			current_interval["file_delivered"] = len(files_completed) # this denotes complete downloads
			current_interval["data"] = data_downloaded
			current_interval["files"] = copy.deepcopy(files_completed)
			current_interval["start_timestamp"] = interval_start_time
			current_interval["end_timestamp"] = time_now
			#update interval start time here
			interval_start_time = time_now
			count = count + 1

			intervals.append(current_interval)



	# update last interval here
	if time_now != interval_start_time:
		current_interval = copy.deepcopy(interval)
		current_interval["number"] = count + 1
		current_interval["file_count"] = len(files_present) # this denotes even partial downloads
		current_interval["file_delivered"] = len(files_completed) # this denotes complete downloads
		current_interval["data"] = data_downloaded
		current_interval["files"] = files_completed
		current_interval["start_timestamp"] = interval_start_time
		current_interval["end_timestamp"] = time_now
		intervals.append(current_interval)

	# write results to a file
	# print intervals
	with open('Results/' + node + '_syncHistory.csv', 'wb') as csv_file:
		writer = csv.writer(csv_file)
		writer.writerow(['Interval #', 'Start timestamp', 'End Timestamp', 
			'Total number of files', 'Data volume', 'Complete transfers', 'Completely downloaded files'])
		for i in range(0,len(intervals)):
			writer.writerow([
				intervals[i]["number"], intervals[i]["start_timestamp"], intervals[i]["end_timestamp"],
				intervals[i]["file_count"], intervals[i]["data"]/1000000.0, intervals[i]["file_delivered"],
				intervals[i]["files"] 
				])
	# print node



"""
Add paths to node dumps 
"""
paths = []
for name in os.listdir('./Dump/'):
	if os.path.isdir(os.path.join('./Dump/', name)):
		name = (os.path.join('./Dump/', name))
		paths.append(name)

# map the names of the nodes corresponding to the file paths
path_to_name = {}
for path in paths:
	# print path
	index = path.rfind('/')
	new_path = path[index + 1:]	
	path_to_name[path] = new_path
# print path_to_name

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
				# print path
				# print file 
				# snapshot_working_dir(path + "/" + file, path_to_name)
	print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXFound ", count, " log files in ", path, "XXXXXXXXXXXXXXXXXXXXXXXXXX"
