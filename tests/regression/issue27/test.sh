#!/usr/bin/env bash

# Related issue: https://github.com/gift-surg/puma/issues/27
# Client code inheriting from BaseRemoteObjectReference was not retrieving the expected type from "self._remote_method" causing Mypy to fail

# Ensure MyPy passes when run in this directory
mypy . --strict
