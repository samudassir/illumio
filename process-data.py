import numpy as np
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

INPUT_FILE = "flowlogs.txt"
PROTOCOL_MAP_FILE = "protocol-numbers.csv"
LOOKUP_FILE = "lookup.txt"


protocol_info = np.loadtxt(PROTOCOL_MAP_FILE, usecols=[0, 1], delimiter=',', dtype='str', skiprows=1, encoding="utf-8")

id_protocol_mp = {}
for d in protocol_info:
    id_protocol_mp[d[0]] = d[1]

flowlog_data = np.loadtxt(INPUT_FILE, usecols=[6, 7], delimiter=' ', dtype='str', encoding="utf-8")

#map protocol keyword instead of referring a number
arr = []
for d in flowlog_data:
    arr.append(id_protocol_mp.get(d[1]))
# data now has TCP, UDP etc mapped  
result = np.insert(flowlog_data, flowlog_data.shape[1], arr, axis=1)


lookup_data = np.loadtxt(LOOKUP_FILE, delimiter=',', dtype='str', skiprows=1, converters={1: lambda x: x.upper(), 2: lambda x: x.strip()}, encoding="utf-8")

portprotocol_tag_map = {}
for d in lookup_data:
    portprotocol_tag_map[tuple(d[0:2])] = d[2]

# create a column for tag based on port and protocol
arr1 = []
for r in result:
    arr1.append(portprotocol_tag_map.get(tuple((r[0],r[2])), "Untag"))

# merge tag column with flow data
result = np.insert(result, result.shape[1], arr1, axis=1)
# delete unused column protocol number
result = np.delete(result, 1, axis=1)

# Count of matches for each tag
unique_values, counts = np.unique(result[:, 2], return_counts=True)
# append it to original data
count_tags = np.column_stack((unique_values, counts))
print("Count of matches for each tag : ")
print(count_tags)

# Count of matches for each port/protocol combination 
result1 = np.delete(result, 2, axis=1)
unique_rows, counts = np.unique(result1, axis=0, return_counts=True)

count_tags = np.column_stack((unique_rows, counts))
print("Count of matches for each port/protocol combination : ")
print(count_tags)