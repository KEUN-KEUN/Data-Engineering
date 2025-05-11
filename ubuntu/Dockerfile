# Ubuntu 20.04를 베이스 이미지로 설정
FROM ubuntu:20.04

# 비인터랙티브 모드 설정 (tzdata 등 설치 시 중단 방지)
ENV DEBIAN_FRONTEND=noninteractive

# 패키지 업데이트 및 유틸리티 설치
RUN apt-get update && \
    apt-get install -y curl vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 기본 쉘로 bash 실행
CMD ["/bin/bash"]