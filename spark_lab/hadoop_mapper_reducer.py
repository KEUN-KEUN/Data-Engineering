import subprocess
import os

HADOOP_HOME = "/home/hadoop-hdfs-user/sw/hadoop-3.3.4"
HDFS_HOST = "localhost"
HDFS_USER = "hadoop-hdfs-user"
INPUT_PATH = f"/user/{HDFS_USER}/wordcount/input.txt"
OUTPUT_PATH = f"/user/{HDFS_USER}/wordcount/output_result"

# mapper.py, reducer.py 파일이 현재 디렉토리에 있다고 가정
MAPPER = "mapper.py"
REDUCER = "reducer.py"

def run_cmd(cmd: list[str]):
    print(f"[RUN] {' '.join(cmd)}")
    result = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    if result.returncode != 0:
        print(f"[ERROR] Command failed: {' '.join(cmd)}")
        print(result.stderr.decode())
        exit(result.returncode)
    return result.stdout.decode()

def main():
    # HDFS 입력 디렉토리 생성 및 파일 업로드
    run_cmd([f"{HADOOP_HOME}/bin/hdfs", "dfs", "-mkdir", "-p", os.path.dirname(INPUT_PATH)])
    run_cmd([f"{HADOOP_HOME}/bin/hdfs", "dfs", "-put", "-f", "input.txt", INPUT_PATH])

    # 이전 출력 디렉토리 제거
    run_cmd([f"{HADOOP_HOME}/bin/hdfs", "dfs", "-rm", "-r", "-f", OUTPUT_PATH])

    # Hadoop Streaming 실행
    run_cmd([
        f"{HADOOP_HOME}/bin/hadoop", "jar",
        f"{HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-3.3.4.jar",
        "-input", INPUT_PATH,
        "-output", OUTPUT_PATH,
        "-mapper", MAPPER,
        "-reducer", REDUCER,
        "-file", MAPPER,
        "-file", REDUCER
    ])

    print(f"✅ Hadoop Streaming WordCount 완료: hdfs://{HDFS_HOST}:9000{OUTPUT_PATH}")

if __name__ == "__main__":
    main()

