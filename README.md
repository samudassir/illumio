## Requirement details ##

* Input file as well as the file containing tag mappings are plain text (ascii) files  
* The flow log file size can be up to 10 MB 
* The lookup file can have up to 10000 mappings 
* The tags can map to more than one port, protocol combinations.  for e.g. sv_P1 and sv_P2 in the sample above. 
* The matches should be case insensitive 

## Solution Summary ##
This program when run will read the flowlog data and lookup data provded for assignment.
I have downloaded protocol information from https://docs.aws.amazon.com/vpc/latest/userguide/flow-log-records.html, and used it to map protocol keyword (TCP, UDP etc.) whereever there is a reference to protocol number in flowlogs.

To keep things simple, I have read only those columns from flow log data which were needed to meet the requirements, which will help minimize data from flowlog file of possibly 10MB.

Flowlog Test Data is tweaked for validations.

Used python's numpy library for all data transformations.

# Step Followed for data processing #
1. Read protocol data and build a map to save "id -> protocol" mapping.
2. Read lookup data and build a map to save "(port,protocol) -> tag" mapping.
3. Read flow log data (col 6, 7) relevant to requirements.
4. Add protocol (ex: TCP, UDP etc) as new column to flowlog data (based on map from step #1).
5. Add tag as new column to flowlog data (based on map from step #2).


## Below steps can be followed to Run the program ##
1. Setup python virtual environment
python3 -m virtualenv venv

2. Activate virtualenv
source ./venv/bin/activate

3. Run process-data.py
python3 process-data.py

4. Deactivate virtualenv
deactivate