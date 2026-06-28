FROM golang:1.22-alpine AS build
WORKDIR /app
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -o /server .

FROM alpine:3.20
COPY --from=build /server /server
ENTRYPOINT ["/server"]
