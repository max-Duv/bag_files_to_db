#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Revision history: 2022 02 16, fixed some comments.
'''
python 3.9

This script parse data in bagfile and then store them into PostgreSQL database.

Accepts a filename as an optional argument. Operates on all bagfiles in current directory if no argument provided

Written by Xinyu Cao at 2023 Nov. at IVSG

Supervised by Professor Sean Brennan
'''

import hashlib
import os
import numpy as np
import datetime
import cv2
import parseUtilities

class parseCamera:

	'''
		============================= Method md5Image() ====================================
		#	Method Purpose:
		#		produce the md5 hash code for string
		#	Input Variable:
		#		img:
		#
		#	Output/Return:
		#		None
		#
		#	Algorithm:
				# initializing string
		str = "GeeksforGeeks"

		# encoding GeeksforGeeks using encode()
		# then sending to md5()
		result = hashlib.md5(str.encode())

		# printing the equivalent hexadecimal value.
		print("The hexadecimal equivalent of hash is : ", end ="")
		print(result.hexdigest())
		#
		# 	Restrictions/Notes:
		# 		None
		#
		# 	The follow methods are called:

		# 	Author: Liming Gao
		# 	Date: 02/05/2020
		#
		================================================================================
	'''

	def __init__(self, folder,bag_file):
		self.folder = folder
		self.bag_file = bag_file
		# self.output_file_name = output_file_name

		'''
		============================= Method make_sure_path_exists() ====================================
		#	Method Purpose:
		#		check if the expected folder exists, if not create one
		#
		#	Input Variable:
		#		self, path
		#
		#	Output/Return:
		#		None
		#
		#	Algorithm:
				create a directory in path
		#
		# 	Restrictions/Notes:
		# 		None
		#
		# 	The follow methods are called:

		# 	Author: Liming Gao
		# 	Date: 02/05/2020
		#
		================================================================================

	'''

	def make_sure_path_exists(self, path):

		try:
			os.makedirs(path)
		except OSError as exception:
			pass
			print('This folder already exists:', path)


	def md5Image(self, img):

		# img.tostring()
		md5Image = hashlib.md5(img.tostring()).hexdigest()

		return md5Image

	'''
		============================= Method saveMD5Image() ====================================
		Method Purpose:
			save img into the folder with hash value filename as .jpg format
		Input Variable:
			img:

		Output/Return:
			None

		Algorithm:
			cv2.imwrite(filename, img[, params])
			cv2.imwrite('img_CV2_90.jpg', a, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
		Restrictions/Notes:
			None

		The follow methods are called:

		Author: Liming Gao
		Date: 02/05/2020

		================================================================================
	'''

	def saveMD5Image(self, image_topic,img):

		md5_filename = self.md5Image(img)
		print (md5_filename)
		# create folder according to hash valus of img
		camera_sub_folder =  image_topic.replace("image_color/compressed","")
		print (camera_sub_folder)
		self.make_sure_path_exists(
			self.folder + '/images/'+ camera_sub_folder + md5_filename[0:2] + '/' + md5_filename[2:4] + '/')
		# create the file name using the hash value of img
		filename = self.folder + '/images/' + camera_sub_folder + \
			md5_filename[0:2] + '/' + md5_filename[2:4] + \
			'/' + md5_filename + '.jpg'
		print (filename)
		# from 0 to 100 (the higher is the better). Default value is 95.
		cv2.imwrite(filename, img, [int(cv2.IMWRITE_JPEG_QUALITY), 100])

		return md5_filename

	def rotateImage(self, img, angle):

		(h, w) = img.shape[:2]
		center = (w/2, h/2)

		M = cv2.retval = cv2.getRotationMatrix2D(center, angle, scale=1.0)
		# M = cv2.retval = cv2.getRotationMatrix2D((w/2, h/2), angle, scale=1.0)
		rotated = cv2.warpAffine(img, M, (w, h))

		return rotated

	def unixTimeToTimeStamp(self, unix_time):

		return datetime.datetime.fromtimestamp(int(unix_time)).strftime('%Y-%m-%d %H:%M:%S')
	
	'''
		============================= Method parseCamera() ====================================
		#	Method Purpose:
		#		parse the Camera data into txt file and inser them into database 
		#
		#	Input Variable:
		#		sensor_id			3,4,5
		#		bag_file_id 		return when insert bag data
		#		bag_file           	bag = rosbag.Bag(bag_file_name)
		# 		camera_info_topic	camera_info_topic = '/front_center_camera/camera_info'
		# 		image_topic			image_topic = '/front_center_camera/image_rect_color/compressed'
		# 		output_file_name_images 	output_file_name_images = folder_name + '/images/' + bag_file_name.replace('.bag', '-front_center_camera-header.txt')
		# 		output_file_name_camera_info	output_file_name_camera_info = folder_name + '/images/' + bag_file_name.replace('.bag', '-front_center_camera-info.txt')
		#
		#
		#	Output/Return:
		#		None
		#
		#	Algorithm:
		#
		#
		# 	Restrictions/Notes:
		#
		#
		# 	The follow methods are called:
		#		parseUtilities.printProgress
		#
		# 	Author: Liming Gao
		# 	Date: 02/05/2020
		#
		================================================================================

	'''

	def parseCamera(self, image_topic, output_file_name_images,rotate=False, angle=0):

		# file = open(output_file_name_camera_info, "w")

		# count = 0
		# for topic, msg, t in bag_file.read_messages(topics=[camera_info_topic]):

		# 	if count == 0:
		# 		file.write(str(msg.width))
		# 		file.write(',')
		# 		file.write(str(msg.height))
		# 		file.write(',')
		# 		file.write(', '.join(map(str, msg.K)))  # This removes the leading and lagging parentheses from this message
		# 		file.write(',')
		# 		file.write(', '.join(map(str, msg.D)))  # This removes the leading and lagging parentheses from this message
		# 		file.write('\n')

		# 		break

		# 		# K_left = np.array(msg.K).reshape((3, 3))
		# 		# D_left = np.array(msg.D)
		# 	count += 1

		# file.close()
		#values=[bag_file_id,sensor_id, msg.K[0], msg.K[4], msg.K[2], msg.K[5], msg.K[1], msg.width, msg.height, msg.D[0], msg.D[1], msg.D[2], msg.D[3], msg.D[4]]

		file = open(output_file_name_images, "w")

		number_of_messages = self.bag_file.get_message_count(topic_filters=image_topic)

		count = 0
		for topic, msg, t in self.bag_file.read_messages(topics=[image_topic]):
			# This must be used for compressed images. CvBridge does not
			# support compressed images.
			# http://wiki.ros.org/rospy_tutorials/Tutorials/WritingImagePublisherSubscriber
			np_arr = np.fromstring(msg.data, np.uint8)
			img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

			if rotate is True:
				img = self.rotateImage(img, angle)

			# This can be used for raw images, but not for compressed. CvBridge
			# does not support compressed images.
			# https://gist.github.com/wngreene/835cda68ddd9c5416defce876a4d7dd9
			# try:
			# 	img = self.bridge.imgmsg_to_cv2(msg)
			# except CvBridgeError, e:
			# 	print e

			# img = cv2.undistort(img,K_left,D_left)

			md5_filename = self.saveMD5Image(image_topic,img)

			# print 'Saving image from ' + topic + ': ' + str(count + 1)

			parseUtilities.printProgress(count + 1, number_of_messages, prefix='Progress:', suffix='Complete', decimals=1, length=50)

			time = repr(msg.header.stamp.secs + msg.header.stamp.nsecs * 10 ** (-9))
			file.write(str(msg.header.seq))
			file.write(',')
			file.write(str(parseUtilities.unixTimeToTimeStamp(msg.header.stamp.secs)))
			file.write(',')
			file.write(str(msg.header.stamp.secs))
			file.write(',')
			file.write(str(msg.header.stamp.nsecs))
			file.write(',')
			file.write(time)
			file.write(',')
			file.write(str(md5_filename))
			file.write('\n')

			count += 1

		file.close()
