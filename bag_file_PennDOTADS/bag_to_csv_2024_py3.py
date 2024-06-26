'''
This script aims to save each topic found in each bag file found in the project directory

It'll accept a filename as an "optional" argument", otherwise it can match ".bag" files in the project directory.

This is a modification of Nick Speal from McGill University's Aerospace Mechatroni Laboratory (www.speal.ca)
and Liming Gao, IVSG (Penn State University, Department of Mechanical Engineering)'s original script.

Modified by Max Duverneuil, IVSG (Penn State University, Information Sciences and Technology on behalf of; Department of Mechanical Engineering, github: mpd5945)

Original Notes: "Notes: flag_camera_parsing =1 ifyou want to parse camera topics into csv (format)."
Additional Notes: As of June 2024, velodyne.decoder library does not accept "rpm", modifications in this program aim to resolve this.

'''




import rosbag, sys, csv
import time
import os
import shutil
import velodyne_decoder as vd
import numpy as np
import hashlib
from parseCamera import parseCamera
from concurrent.futures import ThreadPoolExecutor

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

        if topicName == '/sick_lms_5xx/scan':
            with open(filename, "w") as File:
                for topic, msg, t in bag.read_messages(topicName):
                    File.write(f"{msg.header.seq},{msg.header.stamp.secs},{msg.header.stamp.nsecs},{msg.angle_min},{msg.angle_max},{msg.angle_increment},{msg.time_increment},{msg.scan_time},{msg.range_min},{msg.range_max},{','.join(map(str, msg.ranges))},{','.join(map(str, msg.intensities))}\n")

        elif topicName == '/velodyne_points':
            with open(filename, "w") as File, open(folder + '/velodyne_info.txt', 'w') as InfoFile:
                for topic, msg, t in bag.read_messages(topicName):
                    InfoFile.write(', '.join(map(str, msg.fields)) + '\n')
                    File.write(f"{msg.header.seq},{msg.header.stamp.secs},{msg.header.stamp.nsecs},{msg.height},{msg.width},{msg.is_bigendian},{msg.point_step},{msg.row_step},{msg.is_dense}\n")

        elif topicName == '/velodyne_packets':
            velodyne_folder = folder + '/velodyne_pointcloud'
            os.makedirs(velodyne_folder, exist_ok=True)
            config = vd.Config(model=vd.modelId.VLP_16)
            with open(filename, "w") as File:
                count = 0
                for stamp, points, topic in vd.read_bag(bagName, config, topicName):
                    md5_scan = hashlib.md5(points).hexdigest()
                    sub_folder = velodyne_folder + f'/{md5_scan[:2]}/{md5_scan[2:4]}/'
                    os.makedirs(sub_folder, exist_ok=True)
                    points_file = sub_folder + str(md5_scan) + '.txt'
                    File.write(f"{count},{stamp.secs},{stamp.nsecs},{md5_scan}\n")
                    np.savetxt(points_file, points, delimiter=',')
                    count += 1

        else:
            with open(filename, 'w+') as csvfile:
                filewriter = csv.writer(csvfile, delimiter=',')
                firstIteration = True
                for subtopic, msg, t in bag.read_messages(topicName):
                    msgString = str(msg)
                    msgList = msgString.split('\n')
                    instantaneousListOfData = [pair.split(':') for pair in msgList]
                    if firstIteration:
                        headers = ["rosbagTimestamp"] + [pair[0].strip() for pair in instantaneousListOfData]
                        filewriter.writerow(headers)
                        firstIteration = False
                    values = [str(t)] + [pair[1].strip() for pair in instantaneousListOfData if len(pair) > 1]
                    filewriter.writerow(values)

    bag.close()
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

print(f"Done reading all {len(listOfBagFiles)} bag files.")
print(f"Total time: {total_finish - total_start} seconds.")
