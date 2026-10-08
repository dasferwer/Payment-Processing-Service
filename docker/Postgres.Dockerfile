FROM postgres:17-alpine
# Штатное понижение привилегий выполняет компактный su-exec без Go-зависимостей.
RUN apk upgrade --no-cache \
    && apk add --no-cache su-exec \
    && sed -i 's/exec gosu /exec su-exec /g' /usr/local/bin/docker-entrypoint.sh \
    && rm -f /usr/local/bin/gosu
