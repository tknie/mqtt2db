FROM alpine:latest

COPY mqtt2db /mqtt2db/bin/
COPY docker-entrypoint.sh /

RUN chmod a+x docker-entrypoint.sh

CMD ["/docker-entrypoint.sh"]