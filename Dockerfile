FROM golang:1.5
MAINTAINER Hugo González Labrador

ENV CLAWIO_LOCALSTOREPROP_PORT 57003
ENV CLAWIO_LOCALSTOREPROP_DSN /tmp/clawio-prop.db
ENV CLAWIO_SHAREDSECRET secret

RUN go get -u github.com/clawio/service.localstore.prop

ENTRYPOINT /go/bin/service.localstore.prop

EXPOSE 57003

