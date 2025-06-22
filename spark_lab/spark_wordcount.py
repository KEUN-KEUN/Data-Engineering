from pyspark import SparkContext
import subprocess
import os

sc = SparkContext(appName="WordCount")

HDFS_HOST = "localhost"
HDFS_PORT = 9000
HDFS_USER = "hadoop-hdfs-user"

input_path = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/user/{HDFS_USER}/wordcount/input.txt"
output_path = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/user/{HDFS_USER}/wordcount/output"

# 기존 output 폴더 제거 (디렉토리가 없으면 무시)
hadoop_home = "/home/hadoop-hdfs-user/sw/hadoop-3.3.4"
try:
    result = subprocess.run([
        f"{hadoop_home}/bin/hdfs", "dfs", "-rm", "-r", f"/user/{HDFS_USER}/wordcount/output"
    ], check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)  # stderr를 캡처
except subprocess.CalledProcessError as e:
    # stderr가 None일 수 있기 때문에 조건을 수정하여 처리
    if e.stderr and "No such file or directory" not in e.stderr.decode():  # 디렉토리가 없으면 에러 무시
        print(f"Error occurred while removing output directory: {e.stderr.decode()}")
    else:
        print("Output directory does not exist, skipping removal.")

# WordCount 처리
text_rdd = sc.textFile(input_path)
word_counts = (
    text_rdd.flatMap(lambda line: line.split())
            .map(lambda word: (word.lower(), 1))
            .reduceByKey(lambda a, b: a + b)
)

word_counts.saveAsTextFile(output_path)
print(f"✅ WordCount 결과 저장 완료: {output_path}")

# SparkContext 종료
sc.stop()

