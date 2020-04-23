#!/usr/bin/env bash
# Make python3.7 available via python
update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py
