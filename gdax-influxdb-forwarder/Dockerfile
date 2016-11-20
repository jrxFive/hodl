FROM janeczku/alpine-kubernetes:3.3

WORKDIR /go/src/github.com/jrxfive/cx3/gdax-influxdb-forwarder


COPY ["gdax.go","gdax.go"]
COPY ["vendor/","vendor"]
COPY ["Godeps/","Godeps"]

RUN apk --update add go git \
	&& export GOPATH=/go \
	&& export PATH=$PATH:$GOPATH \
	&& go get -u github.com/tools/godep \
	&& /go/bin/godep restore \
	&& go build -o /bin/gdax-influxdb-forwarder \
	&& apk del go git

CMD ["/bin/gdax-influxdb-forwarder"]