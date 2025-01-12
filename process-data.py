import os
import pandas as pd

INPUT_FILE = "flowlogs.txt"
#read only needed columns
COLS_USED = [6, 7]
CHUNK_SIZE = 5
DATA_FILE = "network_data.csv"


def read_large_file(file_path, chunk_size):
    for chunk in pd.read_table(file_path, sep=' ', header=None, usecols=COLS_USED  ,chunksize=chunk_size):
        yield chunk

#read data in chunks to make optimal use of memory
for data_chunk in read_large_file(INPUT_FILE, CHUNK_SIZE):
    data_chunk.to_csv("network_data.csv", sep=',', mode="a", header=False, index=False)

df1 = pd.read_csv("network_data.csv")
# name cols appropriately
df1.columns = ['dstport', 'protocol']

# read lookup data
lookup_df = pd.read_table("lookup.txt", delimiter=",")
lookup_df['protocol'] = lookup_df['protocol'].str.upper()
lookup_df = lookup_df.rename(columns=lambda x: x.strip())


# mapping data for protocol number and name is taken from here - https://www.iana.org/assignments/protocol-numbers/protocol-numbers.xhtml
protocol_map_df = pd.read_csv("protocol-numbers.csv")
protocol_map_df = protocol_map_df[['Decimal', 'Keyword']]

# map protocol number in input file to protocol name/keyword
formatted_input_df = pd.merge(df1, protocol_map_df, left_on='protocol', right_on='Decimal')
#remove unused cols
del formatted_input_df["protocol"]
del formatted_input_df["Decimal"]
#rename col
formatted_input_df.rename(columns={'Keyword': 'protocol'}, inplace=True)

# merge data from flow logs and lookup
output_df = pd.merge(formatted_input_df, lookup_df, on=['protocol', 'dstport'], how="left")
output_df.fillna({"tag": "Untagged"}, inplace=True)

# verify count based on tags
grouped_df = output_df.groupby('tag')['tag'].count()
print(grouped_df)

# verify count based on port and protocol
grouped_df = output_df.groupby(['dstport', 'protocol']).count()
print(grouped_df)

#clean up
if os.path.exists(DATA_FILE):
  os.remove(DATA_FILE)