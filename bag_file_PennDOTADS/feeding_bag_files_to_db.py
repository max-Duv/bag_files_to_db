'''
This script aims to save each topic found in each bag file found in the project directory

It'll accept a filename as an "optional" argument", otherwise it can match ".bag" files in the project directory.

This is a modification of Nick Speal from McGill University's Aerospace Mechatroni Laboratory (www.speal.ca)
and Liming Gao, IVSG (Penn State University, Department of Mechanical Engineering)'s original script.

Modified by Max Duverneuil, IVSG (Penn State University, Information Sciences and Technology on behalf of; Department of Mechanical Engineering, github: mpd5945)

Original Notes: "Notes: flag_camera_parsing =1 ifyou want to parse camera topics into csv (format)."
Additional Notes: As of June 2024, velodyne.decoder library does not accept "rpm", modifications in this program aim to resolve this.
This script will also connect up to an Azure Database, please exercise caution while running the script as the SQL_DB_USERNAME and SQL_DB_PASSWORD are hardcoded and in plaintext!


'''


import pyodbc
import rosbag
import sys
import time
import os
import shutil
import velodyne_decoder as vd
import numpy as np
import hashlib
from parseCamera import parseCamera
from concurrent.futures import ThreadPoolExecutor

# Database connection details
DSN_NAME = os.getenv('SQL_DSN_NAME', 'ivsg-demo')  # Setup and Download ODBC Administrator for appropriate device
DB_USERNAME = os.getenv('SQL_DB_USERNAME', 'blank')     # Fill in appropriate credentials
DB_PASSWORD = os.getenv('SQL_DB_PASSWORD', 'blank')

try:
    conn_str = f'DSN={DSN_NAME};UID={DB_USERNAME};PWD={DB_PASSWORD}'
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Utility function to process a single bag file
    def process_bag_file(bagFile, flag_camera_parsing):
        start = time.time()
        print(f"Reading file {bagFile}...")

        # Access bag
        bag = rosbag.Bag(bagFile)
        bagContents = bag.read_messages()
        bagName = bag.filename

        # Create a new directory
        folder = bagName.rstrip(".bag")
        os.makedirs(folder, exist_ok=True)

        # Get list of topics from the bag
        listOfTopics = set(topic for topic, _, _ in bagContents)

        # Initialize camera parser
        PC = parseCamera(folder, bag)
        camera_topics = {
            '/rear_left_camera/image_rect_color/compressed',
            '/rear_center_camera/image_rect_color/compressed',
            '/rear_right_camera/image_rect_color/compressed',
            '/front_left_camera/image_color/compressed',
            '/front_center_camera/image_color/compressed',
            '/front_right_camera/image_color/compressed'
        }

        # Parse camera topics if flag is set
        if flag_camera_parsing:
            for topic in camera_topics:
                if topic in listOfTopics:
                    time_start = time.time()
                    OutputFileName = folder + '/' + topic.replace('/', '_slash_') + '.txt'
                    PC.parseCamera(topic, OutputFileName)
                    listOfTopics.remove(topic)
                    print(f"{topic} has been parsed in {time.time() - time_start} seconds.")

        else:
            listOfTopics -= camera_topics

        print(f'For "{bagFile}", these {len(listOfTopics)} topics will be parsed: {listOfTopics}')

        # Process each topic
        for topicName in listOfTopics:
            if topicName == '/sick_lms500/scan' or topicName == '/velodyne_points' or topicName == '/velodyne_packets':
                filename = folder + '/' + topicName.replace('/', '_slash_') + '.txt'
            else:
                filename = folder + '/' + topicName.replace('/', '_slash_') + '.csv'

            if os.path.exists(filename):
                print(f'This file has already existed: {filename}')
                continue
                                                # Below creates necessary DB tables and columns
            if topicName == '/sick_lms_5xx/scan':
                table_name = 'sick_lms_5xx_scan'
                columns = [
                    ('seq', 'INT'),
                    ('secs', 'INT'),
                    ('nsecs', 'INT'),
                    ('angle_min', 'FLOAT'),
                    ('angle_max', 'FLOAT'),
                    ('angle_increment', 'FLOAT'),
                    ('time_increment', 'FLOAT'),
                    ('scan_time', 'FLOAT'),
                    ('range_min', 'FLOAT'),
                    ('range_max', 'FLOAT'),
                    ('ranges', 'NVARCHAR(MAX)'),
                    ('intensities', 'NVARCHAR(MAX)')
                ]
                create_table_sql = f"CREATE TABLE {table_name} (" + ", ".join(f"{col[0]} {col[1]}" for col in columns) + ")"
                cursor.execute(create_table_sql)

                for topic, msg, t in bag.read_messages(topicName):
                    insert_data_sql = f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    cursor.execute(insert_data_sql, (
                        msg.header.seq,
                        msg.header.stamp.secs,
                        msg.header.stamp.nsecs,
                        msg.angle_min,
                        msg.angle_max,
                        msg.angle_increment,
                        msg.time_increment,
                        msg.scan_time,
                        msg.range_min,
                        msg.range_max,
                        ','.join(map(str, msg.ranges)),
                        ','.join(map(str, msg.intensities))
                    ))

            elif topicName == '/velodyne_points':
                table_name = 'velodyne_points'
                columns = [
                    ('seq', 'INT'),
                    ('secs', 'INT'),
                    ('nsecs', 'INT'),
                    ('height', 'INT'),
                    ('width', 'INT'),
                    ('is_bigendian', 'NVARCHAR(10)'),
                    ('point_step', 'INT'),
                    ('row_step', 'INT'),
                    ('is_dense', 'NVARCHAR(10)'),
                    ('fields', 'NVARCHAR(MAX)')
                ]
                create_table_sql = f"CREATE TABLE {table_name} (" + ", ".join(f"{col[0]} {col[1]}" for col in columns) + ")"
                cursor.execute(create_table_sql)

                for topic, msg, t in bag.read_messages(topicName):
                    insert_data_sql = f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    cursor.execute(insert_data_sql, (
                        msg.header.seq,
                        msg.header.stamp.secs,
                        msg.header.stamp.nsecs,
                        msg.height,
                        msg.width,
                        msg.is_bigendian,
                        msg.point_step,
                        msg.row_step,
                        msg.is_dense,
                        ','.join([f"{field.name}:{field.offset}:{field.datatype}:{field.count}" for field in msg.fields])
                    ))

            elif topicName == '/velodyne_packets':
                count = 0
                velodyne_folder = folder + '/velodyne_pointcloud'
                os.makedirs(velodyne_folder, exist_ok=True)
                table_name = 'velodyne_packets'
                create_table_sql = f"CREATE TABLE {table_name} (count INT, secs INT, nsecs INT, md5_scan NVARCHAR(32), points NVARCHAR(MAX))"
                cursor.execute(create_table_sql)

                for stamp, points, topic in vd.read_bag(bagName, topicName):
                    md5_scan = hashlib.md5(points).hexdigest()
                    sub_folder = velodyne_folder + f'/{md5_scan[:2]}/{md5_scan[2:4]}/'
                    os.makedirs(sub_folder, exist_ok=True)
                    points_file = sub_folder + str(md5_scan) + '.txt'
                    np.savetxt(points_file, points, delimiter=',')

                    insert_data_sql = f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)"
                    cursor.execute(insert_data_sql, (
                        count,
                        stamp.secs,
                        stamp.nsecs,
                        md5_scan,
                        points.tolist()
                    ))
                    count += 1

            else:
                table_name = topicName.replace('/', '_slash_')
                create_table_sql = f"CREATE TABLE {table_name} (rosbagTimestamp NVARCHAR(MAX), " \
                                   + ", ".join([f"{pair.split(':')[0].strip()} NVARCHAR(MAX)" for pair in str(next(bag.read_messages(topicName))[1]).split('\n') if len(pair.split(':')) > 1]) + ")"
                cursor.execute(create_table_sql)

                for subtopic, msg, t in bag.read_messages(topicName):
                    msgString = str(msg)
                    msgList = msgString.split('\n')
                    data = {'rosbagTimestamp': str(t)}
                    for pair in msgList:
                        splitPair = pair.split(':')
                        if len(splitPair) > 1:
                            data[splitPair[0].strip()] = splitPair[1].strip()

                    insert_data_sql = f"INSERT INTO {table_name} ({', '.join(data.keys())}) VALUES ({', '.join(['?' for _ in data.values()])})"
                    cursor.execute(insert_data_sql, list(data.values()))

        bag.close()
        conn.commit()
        print(f"Finished {bagFile} in {time.time() - start} seconds.\n")

    # Verify correct input arguments: 1 or 2
    if len(sys.argv) > 2:
        print("Invalid number of arguments: " + str(len(sys.argv)))
        print("Should be 2: 'bag2csv.py' and 'bagName'")
        print("Or just 1: 'bag2csv.py'")
        sys.exit(1)
    elif len(sys.argv) == 2:
        listOfBagFiles = [sys.argv[1]]
    else:
        listOfBagFiles = [f for f in os.listdir(".") if f.endswith(".bag")]
        print(f"Reading all {len(listOfBagFiles)} bagfiles in current directory: {listOfBagFiles}\n")

    # Set flag for camera parsing
    flag_camera_parsing = 1

    # Process all bag files with multi-threading
    total_start = time.time()
    with ThreadPoolExecutor() as executor:
        executor.map(lambda bagFile: process_bag_file(bagFile, flag_camera_parsing), listOfBagFiles)
    total_finish = time.time()

    #Provides user-side confirmation for establishing connection, otherwise renders error. Also provides reads bag file(s) time to complete.
    print(f"Done reading all {len(listOfBagFiles)} bag files.")
    print(f"Total time: {total_finish - total_start} seconds.")

except pyodbc.Error as e:
    print(f"Error connecting to SQL Server: {e}")

