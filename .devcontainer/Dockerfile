FROM mcr.microsoft.com/devcontainers/python:3.14-trixie

RUN apt-get update && apt-get -y upgrade
RUN export DEBIAN_FRONTEND=noninteractive && apt-get -y install --no-install-recommends valkey

