from hdfs import InsecureClient
import os
import platform
import os

print(f"[INFO] Platform: {platform.system()}")
print(f"[INFO] Current Working Directory: {os.getcwd()}")
print(f"[INFO] File exists? {os.path.exists('/opt/shared_data/input.txt')}")


HDFS_URL = 'http://localhost:9870'
HDFS_USER = 'eddi'

client = InsecureClient(HDFS_URL, user=HDFS_USER)

# 경로
local_file_path = '/opt/shared_data/input.txt'
download_dir = '/opt/shared_data/'  # 디렉토리로 지정
downloaded_file_path = os.path.join(download_dir, 'input.txt')  # 이게 생성될 파일

# 파일 생성
os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
with open(local_file_path, 'w') as f:
    f.write("HDFS shared access test.\n")

print("[INFO] 로컬 파일 생성 완료")

# 업로드
client.upload(f'/user/{HDFS_USER}/input.txt', local_file_path, overwrite=True)
print("[INFO] HDFS 업로드 완료")

# 다운로드 (여기서 디렉토리를 지정!)
client.download(f'/user/{HDFS_USER}/input.txt', download_dir, overwrite=True)
print("[INFO] HDFS 다운로드 완료")

# 파일이 존재하는지 확인
if os.path.exists(downloaded_file_path):
    print(f"[✅ 확인 완료] 파일 존재: {downloaded_file_path}")
else:
    print(f"[❌ 에러] 파일 없음: {downloaded_file_path}")
