FROM golang:1.24.5-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/calvin-lab ./cmd/calvin-node ./cmd/calvin-client

# Tek imaj, üç binary: calvin-node (küme düğümü), calvin-client (yük üreticisi
# ve doğrulayıcı), calvin-lab (tek süreçli test ortamı).
FROM alpine:3.22
RUN adduser -D -u 10001 calvin
COPY --from=build /out/ /usr/local/bin/
USER calvin
EXPOSE 7000 2112 2113
CMD ["calvin-node"]
