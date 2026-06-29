FROM golang:1.22-alpine AS build
WORKDIR /app
# Cache the dependency download as its own layer: it only re-runs when
# go.mod/go.sum change, not on every source edit. go.sum is committed, so we
# build straight from it -- no `go mod tidy` (which would also pull test-only
# deps over the network on every build).
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /server .

FROM alpine:3.20
COPY --from=build /server /server
ENTRYPOINT ["/server"]
