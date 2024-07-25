import rosbag
import sys
import os
import velodyne_decoder as vd
import numpy as np
import hashlib

from bag_file_PennDOTADS.parseCamera import parseCamera

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

flag_camera_parsing = 1

for bagFile in listOfBagFiles:
    print(f"Reading file: {bagFile}...")
    bag = rosbag.Bag(bagFile)

    folder = bagFile.rstrip(".bag")
    os.makedirs(folder, exist_ok=True)

    listOfTopics = set(topic for topic, _, _ in bag.read_messages())

    PC = parseCamera(folder, bag)
    camera_topics = {
        '/rear_left_camera/image_rect_color/compressed',
        '/rear_center_camera/image_rect_color/compressed',
        '/rear_right_camera/image_rect_color/compressed',
        '/front_left_camera/image_color/compressed',
        '/front_center_camera/image_color/compressed',
        '/front_right_camera/image_color/compressed'
    }

    if flag_camera_parsing:
        for topic in camera_topics:
            if topic in listOfTopics:
                OutputFileName = folder + '/' + topic.replace('/', '_slash_') + '.txt'
                PC.parseCamera(topic, OutputFileName)

                print(f"{topic} has been parsed.")

    else:
        listOfTopics -= camera_topics

    print(f'For "{bagFile}", these {len(listOfTopics)} topics will be parsed: {listOfTopics}')

    for topicName in listOfTopics:
        filename = folder + '/' + topicName.replace('/', '_slash_') + '.txt'

        if os.path.exists(filename):
            print(f'This file already exists: {filename}')
            continue

        if topicName == '/velodyne_packets':
            velodyne_folder = folder + '/velodyne_pointcloud'
            os.makedirs(velodyne_folder, exist_ok=True)
            config = vd.Config()

            with open(filename, "w") as File:
                count = 0
                for stamp, points, topic, frameID in vd.read_bag(bagFile, config, topicName):
                    print(f"Timestamp: {stamp}")
                    print(f"Points shape: {points.shape}")
                    print(f"Topic: {topic}")
                    print(f"Frame ID: {frameID}")
                    print(f"Points array before making contiguous: {points}")
                    print(
                        f"Shape: {points.shape}, C_CONTIGUOUS: {points.flags['C_CONTIGUOUS']}, F_CONTIGUOUS: {points.flags['F_CONTIGUOUS']}")
                    print(points)

                    points = np.ascontiguousarray(points)
                    print(f"Points array after making contiguous: {points}")
                    print(
                        f"Shape: {points.shape}, C_CONTIGUOUS: {points.flags['C_CONTIGUOUS']}, F_CONTIGUOUS: {points.flags['F_CONTIGUOUS']}")

                    if not points.flags['C_CONTIGUOUS']:
                        raise ValueError("Array is not C-contiguous")

                    md5_scan = hashlib.md5(points.data).hexdigest()
                    sub_folder = f'{velodyne_folder}/{md5_scan[:2]}/{md5_scan[2:4]}/'
                    os.makedirs(sub_folder, exist_ok=True)
                    points_file = sub_folder + md5_scan + '.txt'
                    print(f"Writing to file: {points_file}")
                    File.write(f"{count},{stamp.host},{stamp.device},{md5_scan}\n")
                    np.savetxt(points_file, points, delimiter=',')
                    count += 1

        else:
            pass

    bag.close()

print(f"Done reading all {len(listOfBagFiles)} bag files.")
