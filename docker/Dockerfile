FROM ubuntu:latest

RUN  apt-get update && apt-get -y upgrade

COPY mqtt2db /mqtt2db/bin/
COPY docker-entrypoint.sh /

RUN chmod a+x docker-entrypoint.sh && \
   mkdir /mqtt2db/log

CMD ["/docker-entrypoint.sh"]
