FROM alpine:3.20

RUN apk add --no-cache python3 bash curl ca-certificates util-linux
COPY ktd /usr/local/bin/ktd
RUN chmod +x /usr/local/bin/ktd

EXPOSE 49983
ENTRYPOINT ["/usr/local/bin/ktd", "--listen", "0.0.0.0:49983"]
