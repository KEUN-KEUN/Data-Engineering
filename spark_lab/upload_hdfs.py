from hdfs import InsecureClient
import os

HDFS_USER = "hadoop-hdfs-user"
HDFS_URL = "http://localhost:9870"

local_path = "/opt/shared_data/input.txt"
hdfs_path = f"/user/{HDFS_USER}/wordcount/input.txt"

sample_text = """
Hadoop is an open-source framework.
Hadoop is designed for distributed storage and processing.
Spark is faster than Hadoop MapReduce.
Big data is everywhere.
"""

os.makedirs(os.path.dirname(local_path), exist_ok=True)
with open(local_path, "w") as f:
    f.write(sample_text.strip())

client = InsecureClient(HDFS_URL, user=HDFS_USER)

# Create the directory if it doesn't exist
dir_path = f"/user/{HDFS_USER}/wordcount"
client.makedirs(dir_path)

# Set the permission as a string "777"
client.set_permission(dir_path, permission="777")

# Upload the file to HDFS
client.upload(hdfs_path, local_path, overwrite=True)

print(f"✅ Uploaded to HDFS: {hdfs_path}")

