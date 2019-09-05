FROM golang AS build
WORKDIR /go/src/github.com/mildred/nomadspacedns
COPY . .
ENV CGO_ENABLED 0
ENV GO111MODULE on
RUN go get ./...
RUN go install ./cmd/nsdns

FROM scratch
COPY --from=build /go/bin/nsdns /nsdns
ENV NSDNS_LISTEN_ADDR 0.0.0.0:53
ENTRYPOINT ["/nsdns"]
CMD []
