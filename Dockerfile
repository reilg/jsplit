# syntax=docker/dockerfile:1
FROM golang:1.19

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download
COPY *.go ./

# compile application
RUN go build -o /jsplit

# command to be used to execute when the image is used to start a container
CMD [ "/jsplit" ]