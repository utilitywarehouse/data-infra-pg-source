FROM golang:1-alpine AS build

RUN apk update && apk add make git gcc musl-dev

ARG GITHUB_USER
ARG GITHUB_TOKEN
ARG SERVICE

RUN git config --global url."https://${GITHUB_USER}:${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"

ADD . /app/src/${SERVICE}

WORKDIR /app/src/${SERVICE}

RUN make clean install
RUN make ${SERVICE}

RUN mv ${SERVICE} /${SERVICE}

FROM alpine:latest

ARG SERVICE

ENV APP=${SERVICE}

RUN apk add --no-cache ca-certificates git && mkdir /app
COPY --from=build /${SERVICE} /app/app
COPY --from=build /app/src/${SERVICE}/cmd/${SERVICE}/config.yaml /app/config.yaml

CMD [ "/app/app", "-c" ,"/app/config.yaml" ]