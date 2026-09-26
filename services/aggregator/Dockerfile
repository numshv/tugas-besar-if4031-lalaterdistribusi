FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY pkg ./pkg
COPY services/aggregator ./services/aggregator
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./services/aggregator

FROM alpine:3.21
RUN adduser -D -u 10001 app
COPY --from=build /out/app /usr/local/bin/app
USER app
EXPOSE 8083
ENTRYPOINT ["/usr/local/bin/app"]
