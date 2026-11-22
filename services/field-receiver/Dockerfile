FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY pkg ./pkg
COPY services/field-receiver ./services/field-receiver
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./services/field-receiver

FROM alpine:3.21
RUN adduser -D -u 10001 app
COPY --from=build /out/app /usr/local/bin/app
USER app
EXPOSE 8089
ENTRYPOINT ["/usr/local/bin/app"]
