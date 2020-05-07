#!/usr/bin/env bash

# Related issue: https://github.com/gift-surg/puma/issues/29
# Logging config files are not copied to the install directory, causing init_logging() to fail

# Ensure Python script is able to run successfully
python ./initialise_logging.py
