Ivsg.psu SU’24 | Max Duverneuil | Jun 26, 2024| feeding_bag_files_to_db.py

Task: Parse out core position data - GPS, encoders, time trigger. Write Python scripts.

Overview:
Modifications to the bag_to_csv_py3.py file were made to ensure runtime and modern implementations of broken Python library functions. 

This documentation details the necessary work and dependencies needed to parse and insert bag files from a user's project directory into a Microsoft Azure free SQL Transact Database.

Dependencies:
Python3 installed
pip install: pyodbc, velodyne_decoder, bagpy, rosbag
~optional~ Python interpreter: 
The latest version of PyCharm is recommended 
Microsoft Azure SQL database
The latest version of Microsoft OBDC Data Source Administrator setup
Use both User DSN and System DSN when setting up the Database drivers.

Run:

Open a terminal and run the following script:

` python3 feeding_bag_files_to_db.py /*insert*/*your*/*proj*/*dir*/*here* `

Make sure you have at least one bag file in your project directory otherwise, it will fail. Also, ensure that your login credentials are appropriately configured to connect to the Azure database. 



