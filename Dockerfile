FROM golang:1.24.5-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/calvin-lab ./cmd/calvin-lab

FROM alpine:3.22
RUN adduser -D -u 10001 calvin
COPY --from=build /out/calvin-lab /usr/local/bin/calvin-lab
USER calvin
EXPOSE 2112
ENTRYPOINT ["calvin-lab"]
