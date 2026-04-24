FROM postgres:16-alpine

RUN apk add --no-cache dcron tini

COPY pg_backup.sh /usr/local/bin/pg_backup.sh
RUN chmod +x /usr/local/bin/pg_backup.sh

RUN echo "0 3 * * * /usr/local/bin/pg_backup.sh >> /var/log/backup.log 2>&1" > /etc/crontabs/root

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["sh", "-c", "touch /var/log/backup.log && crond -f -l 8 & tail -f /var/log/backup.log"]
