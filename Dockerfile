# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/rpchub ./cmd/rpchub

FROM alpine:3.21
RUN apk add --no-cache ca-certificates \
	&& adduser -D -H rpchub \
	&& mkdir -p /data && chown rpchub /data
COPY --from=build /out/rpchub /usr/local/bin/rpchub
USER rpchub
ENV PORT=9563 CACHE_DIR=/data
EXPOSE 9563
# start-period matches the app's /health warm-up grace
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s \
	CMD wget -qO- http://127.0.0.1:9563/health >/dev/null || exit 1
ENTRYPOINT ["rpchub"]
