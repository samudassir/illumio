import numpy as np
from typing import List, Dict
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

INPUT_FILE = "flowlogs.txt"
LOOKUP_FILE = "lookup.txt"
PROTOCOL_MAP_FILE = "protocol-numbers.csv"

#downloaded protocol info from https://docs.aws.amazon.com/vpc/latest/userguide/flow-log-records.html   
def read_protocol_info() -> Dict:
    """read protocol information""" 
    protocol_info = np.loadtxt(PROTOCOL_MAP_FILE, usecols=[0, 1], delimiter=',', dtype='str', skiprows=1, encoding="utf-8")
    # use a map for protocol info for fast retrieval
    id_protocol_mp = {}
    for protocol_id, protocol in protocol_info:
        id_protocol_mp[protocol_id] = protocol
    return id_protocol_mp

def read_lookup_data() -> Dict:
    """read lookup data"""
    lookup_data = np.loadtxt(LOOKUP_FILE, delimiter=',', dtype='str', skiprows=1, converters={1: lambda x: x.upper(), 2: lambda x: x.strip()}, encoding="utf-8")
    #build a map to easily retrieve tag name based on "port and protocol" as key
    portprotocol_tag_map = {}
    for d in lookup_data:
        # dstport, protocol -> tag
        portprotocol_tag_map[tuple(d[0:2])] = d[2]
    return portprotocol_tag_map

def read_flowlog_data() -> List[List]:
    """Function to process flow log data and return 2D array appending tags column"""
    id_protocol_mp = read_protocol_info()
    portprotocol_tag_map = read_lookup_data()

    # read flow log data, load only specific columns which are needed
    flowlog_data = np.loadtxt(INPUT_FILE, usecols=[6, 7], delimiter=' ', dtype='str', encoding="utf-8")

    # build an array to be added to flowlog data, refer protocol keywork like TCP, UDP etc, instead of a number
    arr = []
    for _, protocol_id in flowlog_data:
        arr.append(id_protocol_mp.get(protocol_id))
    # data now has TCP, UDP etc mapped  
    flowlog_data = np.insert(flowlog_data, flowlog_data.shape[1], arr, axis=1)
    # build an array to add tag column to flow log data
    tags_column = []
    for port ,_, protocol in flowlog_data:
        tags_column.append(portprotocol_tag_map.get(tuple((port,protocol)), "Untag"))

    # merge tag column with flow data
    result = np.insert(flowlog_data, flowlog_data.shape[1], tags_column, axis=1)
    # delete unused column "protocol number"
    result = np.delete(result, 1, axis=1)
    return result



if __name__ == "__main__":
    result = read_flowlog_data()
    # Count of matches for each tag
    unique_values, counts = np.unique(result[:, 2], return_counts=True)
    # append summary
    count_tags = np.column_stack((unique_values, counts))
    print("Count of matches for each tag : ")
    print("[TAG, COUNT]")
    print(count_tags)

    # Count of matches for each port/protocol combination 
    result1 = np.delete(result, 2, axis=1)
    unique_rows, counts = np.unique(result1, axis=0, return_counts=True)
    # append summary
    count_tags = np.column_stack((unique_rows, counts))
    print("Count of matches for each port/protocol combination : ")
    print("[PORT, PROTOCOL, COUNT]")
    print(count_tags)