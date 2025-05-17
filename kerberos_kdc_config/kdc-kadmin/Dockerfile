FROM debian:bookworm

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -qq && \
    apt-get install -qq -y krb5-kdc krb5-admin-server locales && \
    apt-get clean

RUN locale-gen en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

COPY init-script.sh /tmp/
RUN chmod +x /tmp/init-script.sh

CMD ["/tmp/init-script.sh"]
