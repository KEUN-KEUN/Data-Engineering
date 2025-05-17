# Kerberos KDC Docker 컨테이너 설정

이 문서는 **Kerberos KDC 컨테이너**에서 `keytab` 파일을 호스트 시스템으로 복사하는 방법에 대해 설명합니다.

## Docker 환경 구성

### 1. Docker 컨테이너에서 `keytab` 파일 복사

다음 명령어를 사용하여 **Kerberos KDC 컨테이너** 내의 `service.keytab` 파일을 **호스트 시스템**으로 복사할 수 있습니다.


```bash
docker cp kerberos-kdc-container:/keytabs/service.keytab ./service.keytab
```

