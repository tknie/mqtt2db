FROM alpine:latest

RUN  apk update && apk upgrade && apk add gcompat

COPY mqtt2db /mqtt2db/bin/
COPY docker-entrypoint.sh /

RUN chmod a+x docker-entrypoint.sh && \
   mkdir /mqtt2db/log && chmod 777 /mqtt2db/log

ENV LOGFILE=/mqtt2db/log/

CMD ["/docker-entrypoint.sh"]
