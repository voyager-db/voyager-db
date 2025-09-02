# syntax=docker/dockerfile:1
FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod .
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/voyagerd ./cmd/voyagerd && \
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/voyagerctl ./cmd/voyagerctl


FROM gcr.io/distroless/base
COPY --from=build /out/voyagerd /usr/local/bin/voyagerd
COPY --from=build /out/voyagerctl /usr/local/bin/voyagerctl
ENTRYPOINT ["/usr/local/bin/voyagerd"]