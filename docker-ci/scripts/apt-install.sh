#!/usr/bin/env bash
apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -y \
        tzdata \
        python3.7 \
        python3-distutils \
        libpython3.7 \
        libxxf86vm1 \
        curl \
        git-core \
        emacs \
        xvfb \
    && rm -rf /var/lib/apt/lists/*
