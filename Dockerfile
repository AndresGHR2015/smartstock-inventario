FROM golang:1.24-bookworm AS build
WORKDIR /src
ENV GOFLAGS=-mod=mod
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /inventory ./cmd/inventory

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /inventory /inventory
EXPOSE 50051
ENTRYPOINT ["/inventory"]
