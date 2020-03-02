#! /bin/bash

########################################################################
## Set global variables and methods to be used across the benchmark   ##
## scripts.                                                           ##
########################################################################

# Hadoop cluster address
HADOOP_CLUSTER="-fs hdfs://hadoop01.sindice.net:9000 -jt hadoop01.sindice.net:9001"

COLOR_RED='\e[00;31m'
COLOR_GREEN='\e[00;32m'
COLOR_END='\e[00m'

## Check that the mandatory option has been used
function _req_option() {
    if [[ $1 -eq 1 ]]; then
        echo "Missing parameter -$2"
        _usage
        exit 1
    fi
}
