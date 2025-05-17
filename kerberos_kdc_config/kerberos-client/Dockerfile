FROM debian:bookworm

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -qq && \
    apt-get install -qq -y krb5-user curl && \
    apt-get clean

COPY configureKerberosClient.sh /tmp/
RUN chmod +x /tmp/configureKerberosClient.sh

CMD ["/tmp/configureKerberosClient.sh"]
