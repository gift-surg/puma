FROM ubuntu:18.04
COPY scripts /tmp/scripts
RUN /tmp/scripts/apt-install.sh && rm -f /tmp/scripts/apt-install.sh
RUN /tmp/scripts/setup-python.sh && rm -f /tmp/scripts/setup-python.sh
