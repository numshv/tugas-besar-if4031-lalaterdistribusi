FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY pkg ./pkg
COPY services/aggregator ./services/aggregator
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./services/aggregator
RUN CGO_ENABLED=0 go build -trimpath -o /out/loader ./services/aggregator/cmd/loader

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=build /out/app /usr/local/bin/app
COPY --from=build /out/loader /usr/local/bin/loader
USER app
EXPOSE 8083
ENTRYPOINT ["/usr/local/bin/app"]
