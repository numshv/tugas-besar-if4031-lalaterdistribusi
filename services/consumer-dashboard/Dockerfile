FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY pkg ./pkg
COPY services/consumer-dashboard ./services/consumer-dashboard
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./services/consumer-dashboard

FROM alpine:3.21
RUN adduser -D -u 10001 app
COPY --from=build /out/app /usr/local/bin/app
USER app
EXPOSE 8085
ENTRYPOINT ["/usr/local/bin/app"]
