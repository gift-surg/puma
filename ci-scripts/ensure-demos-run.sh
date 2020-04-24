#!/usr/bin/env bash

# Exit the script if any command fails
set -e

# Define colours for pretty output
NC="\033[0m"           # No colour
RED="\033[0;31m"
GREEN="\033[0;32m"
BLUE="\033[0;34m"

# Find all demos
FAILURES=0
IFS=$'\n'
for executable in $(find . -type f -iname "*demo.py");
do
    echo -e "${BLUE}Checking execution of: ${executable}${NC}"
    set +e
    RESPONSE=$(timeout 180 python ${executable} 2>&1)
    STATUS=$?
    echo "${RESPONSE}"
    set -e

    if [[ ${STATUS} -eq 0 ]]; then
        echo -e "${GREEN}OK${NC}"
    else
        if [[ ${STATUS} -eq 124 ]]; then
            failed_msg="TIMED OUT";
        else
            failed_msg="FAILED";
        fi
        echo -e "${RED}${failed_msg}${NC}"
        FAILURES=$((FAILURES + 1))
    fi
done

echo

# Show any errors and determine exit code
EXIT_CODE=0
if [[ ${FAILURES} -eq 0 ]]; then
    echo -e "${GREEN}All demos ran successfully${NC}"
else
    echo -e "${RED}${FAILURES} executable(s) failed to run${NC}"
    ((EXIT_CODE++))
fi

exit ${EXIT_CODE}
