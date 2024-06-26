'''
This script saves each topic in a bagfile as a csv.

Accepts a filename as an optional argument. Operates on all bagfiles in current directory if no argument provided

Created by Nick Speal in May 2013 at McGill University's Aerospace Mechatronics Laboratory www.speal.ca
Modified by Liming Gao,IVSG

Edited by Wushuang Bai 2022 10 24 to write it into a python3 version.

Notes: flag_camera_parsing =1 if you want to parse camera topics into csv.

'''

import rosbag, sys, csv #pip3 install bagpy
import time
import string
import os #for file management make directory
import shutil #for file management, copy file
import velodyne_decoder as vd #pip install velodyne-decoder
import numpy as np
import hashlib
from parseCamera import parseCamera

#verify correct input arguments: 1 or 2
if (len(sys.argv) > 2):
	print ("invalid number of arguments:   " + str(len(sys.argv)))
	print ("should be 2: 'bag2csv.py' and 'bagName'")
	print ("or just 1  : 'bag2csv.py'")
	sys.exit(1)
elif (len(sys.argv) == 2):
	listOfBagFiles = [sys.argv[1]]
	numberOfFiles = "1"
	print ("reading only 1 bagfile: " + str(listOfBagFiles[0]))
elif (len(sys.argv) == 1):
	listOfBagFiles = [f for f in os.listdir(".") if f[-4:] == ".bag"]	#get list of only bag files in current dir.
	numberOfFiles = str(len(listOfBagFiles))
	print ("reading all " + numberOfFiles + " bagfiles in current directory: \n")
	for f in listOfBagFiles:
		print (f)
	print ("\n press ctrl+c in the next 5 seconds to cancel \n")
	time.sleep(5)
else:
	print ("bad argument(s): " + str(sys.argv))	#shouldnt really come up
	sys.exit(1)

##
flag_camera_parsing = 1


# Choose timer to use
# if sys.platform.startswith('win'):
# 	default_timer = time.clock
# else:
default_timer = time.time

total_start = default_timer()


count = 0
for bagFile in listOfBagFiles:
	count += 1
	start = default_timer()
	print ("reading file " + str(count) + " of  " + numberOfFiles + ": " + bagFile + "...")
	#access bag
	bag = rosbag.Bag(bagFile)
	bagContents = bag.read_messages()
	bagName = bag.filename


	#create a new directory
	# folder = string.rstrip(bagName, ".bag")
	folder = bagName.rstrip(".bag")
	try:	#else already exists
		os.makedirs(folder)
	except:
		pass
		print ('this folder already exists:', folder)
	#shutil.copyfile(bagName, folder + '/' + bagName)


	#get list of topics from the bag
	listOfTopics = []
	for topic, msg, t in bagContents:
		if topic not in listOfTopics:
			listOfTopics.append(topic)
	PC = parseCamera(folder,bag)
	if flag_camera_parsing == 1:
		if '/rear_left_camera/image_rect_color/compressed' in listOfTopics:
			time_start = time.time()
			rear_left_image_topic = '/rear_left_camera/image_rect_color/compressed'
			OutputFileName = folder + '/' + rear_left_image_topic.replace('/', '_slash_') + '.txt'
			PC.parseCamera(rear_left_image_topic, OutputFileName)
			listOfTopics.remove(rear_left_image_topic)
			time_end = time.time()
			time_elpased = time_end - time_start
			print("'rear_left_camera' has been parsed.")
			print("Elpased time is " + str(time_elpased))
		if '/rear_center_camera/image_rect_color/compressed' in listOfTopics:
			time_start = time.time()
			rear_center_image_topic = '/rear_center_camera/image_rect_color/compressed'
			OutputFileName = folder + '/' + rear_center_image_topic.replace('/', '_slash_') + '.txt'
			PC.parseCamera(rear_center_image_topic, OutputFileName)
			listOfTopics.remove(rear_center_image_topic)
			print("'rear_center_camera' has been parsed.")
			time_end = time.time()
			time_elpased = time_end - time_start
		if '/rear_right_camera/image_rect_color/compressed' in listOfTopics:
			time_start = time.time()
			rear_right_image_topic = '/rear_right_camera/image_rect_color/compressed'
			OutputFileName = folder + '/' + rear_right_image_topic.replace('/', '_slash_') + '.txt'
			PC.parseCamera(rear_right_image_topic, OutputFileName)
			listOfTopics.remove(rear_right_image_topic)
			print("'rear_right_camera' has been parsed.")
			time_end = time.time()
			time_elpased = time_end - time_start
		if '/front_left_camera/image_color/compressed' in listOfTopics:
			front_left_image_topic = '/front_left_camera/image_color/compressed'
			OutputFileName = folder + '/' + front_left_image_topic.replace('/', '_slash_') + '.txt'
			PC.parseCamera(front_left_image_topic, OutputFileName)
			listOfTopics.remove(front_left_image_topic)
			print("'front_left_camera' has been parsed.")
		if '/front_center_camera/image_color/compressed' in listOfTopics:
			front_center_image_topic = '/front_center_camera/image_color/compressed'
			OutputFileName = folder + '/' + front_center_image_topic.replace('/', '_slash_') + '.txt'
			PC.parseCamera(front_center_image_topic, OutputFileName)
			listOfTopics.remove(front_center_image_topic)
			print("'front_center_camera' has been parsed.")
		if '/front_right_camera/image_color/compressed' in listOfTopics:
			front_right_image_topic = '/front_right_camera/image_color/compressed'
			OutputFileName = folder + '/' + front_right_image_topic.replace('/', '_slash_') + '.txt'
			PC.parseCamera(front_right_image_topic, OutputFileName)
			listOfTopics.remove(front_right_image_topic)
			print("'front_right_camera' has been parsed.")

		if '/rear_left_camera/image_rect_color' in listOfTopics:
			listOfTopics.remove('/rear_left_camera/image_rect_color')
			print("'rear_left_camera' will not be parsed.")
		if '/rear_center_camera/image_rect_color' in listOfTopics:
			listOfTopics.remove('/rear_center_camera/image_rect_color')
			print("'rear_center_camera' will not be parsed.")
		if '/rear_right_camera/image_rect_color' in listOfTopics:
			listOfTopics.remove('/rear_right_camera/image_rect_color')
			print("'rear_right_camera' will not be parsed.")

		if '/rear_left_camera/image_color/compressed' in listOfTopics:
			listOfTopics.remove('/rear_left_camera/image_color/compressed')
			print("'rear_left_camera' will not be parsed.")
		if '/rear_center_camera/image_color/compressed' in listOfTopics:
			listOfTopics.remove('/rear_center_camera/image_color/compressed')
			print("'rear_center_camera' will not be parsed.")
		if '/rear_right_camera/image_color/compressed' in listOfTopics:
			listOfTopics.remove('/rear_right_camera/image_color/compressed')
			print("'rear_right_camera' will not be parsed.")
	else:
		if '/rear_left_camera/image_rect_color/compressed' in listOfTopics:
			listOfTopics.remove('/rear_left_camera/image_rect_color/compressed')
			print("'rear_left_camera' will not be parsed.")
		if '/rear_center_camera/image_rect_color/compressed' in listOfTopics:
			listOfTopics.remove('/rear_center_camera/image_rect_color/compressed')
			print("'rear_center_camera' will not be parsed.")
		if '/rear_right_camera/image_rect_color/compressed' in listOfTopics:
			listOfTopics.remove('/rear_right_camera/image_rect_color/compressed')
			print("'rear_right_camera' will not be parsed.")
		if '/front_left_camera/image_rect_color/compressed' in listOfTopics:
			listOfTopics.remove('/front_left_camera/image_rect_color/compressed')
			print("'front_left_camera' will not be parsed.")
		if '/front_center_camera/image_rect_color/compressed' in listOfTopics:
			listOfTopics.remove('/front_center_camera/image_rect_color/compressed')
			print("'front_center_camera' will not be parsed.")
		if '/front_right_camera/image_rect_color/compressed' in listOfTopics:
			listOfTopics.remove('/front_right_camera/image_rect_color/compressed')
			print("'front_right_camera' will not be parsed.")



	#if count ==1: #print the content of the list
	print ('For "{}", these {} topics will be parsed: \n{}'.format(bagFile,len(listOfTopics),listOfTopics))
	
	for topicName in listOfTopics:
		#Create a new CSV file for each topic
		if topicName == '/sick_lms500/scan' or topicName == '/velodyne_points' or topicName == '/velodyne_packets': #convert this topic into txt file 
			# filename = folder + '/' + string.replace(topicName, '/', '_slash_') + '.txt'
			filename = folder + '/' + topicName.replace('/', '_slash_') + '.txt'
		else:
			filename = folder + '/' + topicName.replace('/', '_slash_') + '.csv'

		if not os.path.exists(filename):

			if topicName == '/sick_lms_5xx/scan': #convert this topic into txt file 
				
				# OutputFileName = folder + '/' + string.replace(topicName, '/', '_slash_') + '.txt'
				OutputFileName = folder + '/' + topicName.replace('/', '_slash_') + '.txt'
				File = open(OutputFileName,"w")
				#print("Parsing Laser...")
				for topic, msg, t in bag.read_messages(topicName):
				#	print msg
					File.write(str(msg.header.seq))
					File.write(',')
					File.write(str(msg.header.stamp.secs))
					File.write(',')
					File.write(str(msg.header.stamp.nsecs))
					File.write(',')
					File.write(str(msg.angle_min))
					File.write(',')
					File.write(str(msg.angle_max))
					File.write(',')
					File.write(str(msg.angle_increment))
					File.write(',')
					File.write(str(msg.time_increment))
					File.write(',')
					File.write(str(msg.scan_time))
					File.write(',')
					File.write(str(msg.range_min))
					File.write(',')
					File.write(str(msg.range_max))
					File.write(',')
					File.write(', '.join(map(str,msg.ranges))) # This removes the leading and lagging parenthese from this message
					File.write(',')
					File.write(', '.join(map(str,msg.intensities))) # This removes the leading and lagging parenthese from this message
					File.write('\n')

				File.close()
			elif topicName == '/velodyne_points':
				OutputFileName = folder + '/' + topicName.replace('/', '_slash_') + '.txt'
				File = open(OutputFileName,"w")
				VelodyneInfoFile = folder +'/'+'velodyne_info.txt'
				InfoFile = open(VelodyneInfoFile,'w')
				for topic, msg, t in bag.read_messages(topicName):

					InfoFile.write(', '.join(map(str,msg.fields))) # This removes the leading and lagging parenthese from this message
					InfoFile.write('\n')
					File.write(str(msg.header.seq))
					File.write(',')
					File.write(str(msg.header.stamp.secs))
					File.write(',')
					File.write(str(msg.header.stamp.nsecs))
					File.write(',')
					File.write(str(msg.height))
					File.write(',')
					File.write(str(msg.width))
					File.write(',')
					File.write(str(msg.is_bigendian))
					File.write(',')
					File.write(str(msg.point_step))
					File.write(',')
					File.write(str(msg.row_step))
					File.write(',')
					File.write(str(msg.is_dense))
					File.write('\n')

				File.close()
				InfoFile.close()

			elif topicName == '/velodyne_packets':
				count = 0
				velodyne_folder = folder + '/' + 'velodyne_pointcloud'
				config = vd.Config(model = "VLP-16", rpm = 600)
				try:	#else already exists
					os.makedirs(velodyne_folder)
				except:
					pass
					print ('this folder already exists:', folder)
				lidar_topics = [topicName]
				cloud_arrays = []
				OutputFileName = folder + '/' + topicName.replace('/', '_slash_') + '.txt'
				File = open(OutputFileName,"w")
				for stamp, points, topic in vd.read_bag(bagName, config, topicName):
					md5_scan = hashlib.md5(points).hexdigest()
					sub_folder = velodyne_folder + '/' + md5_scan[0:2] + '/' + md5_scan[2:4] + '/'
					points_file = sub_folder + str(md5_scan) + '.txt'
					try:
						os.makedirs(sub_folder)
					except:
						pass
						print ('this folder already existsL:', sub_folder)
					
					File.write(str(count))
					File.write(',')
					File.write(str(stamp.secs))
					File.write(',')
					File.write(str(stamp.nsecs))
					File.write(',')
					File.write(str(md5_scan))
					File.write('\n')
					np.savetxt(points_file, points, delimiter=',')

					cloud_arrays.append(points)

					count += 1
					
				File.close()

			else:
				with open(filename, 'w+') as csvfile:
					filewriter = csv.writer(csvfile, delimiter = ',')
					firstIteration = True	#allows header row
					for subtopic, msg, t in bag.read_messages(topicName):	# for each instant in time that has data for topicName
						#parse data from this instant, which is of the form of multiple lines of "Name: value\n"
						#	- put it in the form of a list of 2-element lists
						msgString = str(msg)
						# msgList = string.split(msgString, '\n')
						msgList = msgString.split('\n')
						instantaneousListOfData = []
						for nameValuePair in msgList:
							# splitPair = string.split(nameValuePair, ':')
							splitPair = nameValuePair.split(':')
							for i in range(len(splitPair)):	#should be 0 to 1
								# splitPair[i] = string.strip(splitPair[i])
								splitPair[i] = splitPair[i].strip()
							instantaneousListOfData.append(splitPair)
						#write the first row from the first element of each pair
						if firstIteration:	# header
							headers = ["rosbagTimestamp"]	#first column header
							for pair in instantaneousListOfData:
								headers.append(pair[0])
							filewriter.writerow(headers)
							firstIteration = False
						# write the value from each pair to the file
						values = [str(t)]	#first column will have rosbag timestamp
						for pair in instantaneousListOfData:
							if len(pair) > 1:
								values.append(pair[1])
						filewriter.writerow(values)
		else:
			print ('This file has already existed:', filename)

	bag.close()



	finish = default_timer()

	print ("Finished " + bagFile + " in " + str(finish-start) + " seconds.\n")

print ("Done reading all " + numberOfFiles + " bag files.")

total_finish = default_timer()

print ("Total time: " + str(total_finish-total_start) + " seconds.")
