FROM ubuntu:18.04 AS PUMA_CI
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        tzdata \
        git-core \
        python3.7 \
        python3-distutils \
        curl \
    && rm -rf /var/lib/apt/lists/*
# Make python3.7 available via python
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN python get-pip.py
# Install needed tools
RUN pip install pytest
RUN pwd
