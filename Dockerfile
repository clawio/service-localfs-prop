FROM golang:1.5
MAINTAINER Hugo González Labrador

ENV CLAWIO_LOCALFS_PROP_PORT 57003
ENV CLAWIO_LOCALFS_PROP_DSN "prop:passforuserprop@tcp(service-localfs-prop-mysql:57005)/prop"
ENV CLAWIO_LOCALFS_PROP_MAXSQLIDLE 1024
ENV CLAWIO_LOCALFS_PROP_MAXSQLCONCURRENCY 1024
ENV CLAWIO_LOCALFS_PROP_LOGLEVEL "error"
ENV CLAWIO_SHAREDSECRET secret

ADD . /go/src/github.com/clawio/service-localfs-prop
WORKDIR /go/src/github.com/clawio/service-localfs-prop

RUN go get -u github.com/tools/godep
RUN godep restore
RUN go install

ENTRYPOINT /go/bin/service-localfs-prop

EXPOSE 57003

