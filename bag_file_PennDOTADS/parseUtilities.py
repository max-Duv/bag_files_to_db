#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime


def parseBagFileNameForDateTime(file_name):

	# file_name: mapping_van_2019-10-18-20-39-30_12.bag
	date_time = file_name.split('_')[2]
	date_time = date_time.split('-')
	date_time = '-'.join(date_time[0:3]) + ' ' + ':'.join(date_time[3:6])

		# date_time: 2019-10-18 20:39:30
	return date_time

def parseBagFileNameForSplitFileIndex(file_name):

	# file_name: mapping_van_2019-10-18-20-39-30_12.bag
	split_file_index = file_name.split('_')[3]
	# print (split_file_index)
	split_file_index = split_file_index.split('.')[0]
	# print (split_file_index)
	return int(split_file_index)  # 12

def unixTimeToTimeStamp(unix_time):

	return datetime.datetime.fromtimestamp(int(unix_time)).strftime('%Y-%m-%d %H:%M:%S')

		# input 0, output 1970-01-01 00:00:00

	'''
		a.How to Print Without Newline? python2, print is a statment, pyhton3 print is a function
			1. for python3, use end= 
				print("Hello World!", end = '')
				print("My name is Karim")
				# output:
				# Hello World!My name is Karim
			2. for python2, use a comma at the end of your print statement
				print "Hello World!",
				print "My name is Karim"
				# output
				# Hello World! My name is Karim

		b.Print iterations progress
		# http://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console/34325723#34325723
		# https://gist.github.com/aubricus/f91fb55dc6ba5557fbab06119420dd6a
	'''

def printProgress(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█'):
	"""
	Call in a loop to create terminal progress bar
	@params:
		iteration   - Required  : current iteration (Int)
		total       - Required  : total iterations (Int)
		prefix      - Optional  : prefix string (Str)
		suffix      - Optional  : suffix string (Str)
		decimals    - Optional  : positive number of decimals in percent complete (Int)
		length      - Optional  : character length of bar (Int)
		fill        - Optional  : bar fill character (Str)
	"""

	percent = ("{0:." + str(decimals) + "f}").format(100 *(iteration / float(total)))
	filledLength = int(length * iteration // total)
	bar = fill * filledLength + '-' * (length - filledLength)
	print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r') # python 3
	# print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix),) # python 2

	# Print New Line on Complete
	if iteration == total:
		print()