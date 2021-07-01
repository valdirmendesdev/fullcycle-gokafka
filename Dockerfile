FROM golang:1.16

WORKDIR /go/src

ENV PATH="/go/bin:${PATH}"

RUN apt-get update && \
  apt-get install build-essential librdkafka-dev -y

cmd ["tail", "-f","/dev/null"] 