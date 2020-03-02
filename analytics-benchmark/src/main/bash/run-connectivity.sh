#! /bin/bash

########################################################################
## This script runs the connectivity precision evaluation over the    ##
## given list of datasets and graph summarization algorithms.         ##
##                                                                    ##
## This script relies on a specific directory structure about the     ##
## graph summarization outputs. Given a root folder named <ROOT>, it  ##
## expects to find the following layout:                              ##
##     <DATASET_NAME>/<DATE>/<ALGORITHM>-run-<X>                      ##
## where we have in order the name of the dataset, the date when the  ##
## Hadoop job started with format yyyy-mm-dd, the name of the graph   ##
## summarization algorithm, and <X> identifies multiple runs of the   ##
## Hadoop job on that date.                                           ##
########################################################################

source common.sh

###
### Variables
###
WORKDIR=$(dirname $0)
GOLD_STANDARD="F_BISIMULATION"

###
### Options variables
###
INPUT="The input folder, on HDFS"
DATASETS="" # The list of dataset to process
ALGORITHMS="" # The list of algorithms to evaluate

HADOOP_BIN="The path to the hadoop bin script"
CONFIG_YAML="The YAML configuration file of the summary cascade"
JAR="The path to the JAR file"
D_OPTIONS="" # list of hadoop parameters

## Print the usage description of the script
function _usage() {
cat <<EOF
    Welcome to the Data Graph Summary benchmark CLI - Connectivity Precision.
    Here are the options (options with * are mandatory):
        -h  : print this help.
        -i* : input directory on HDFS, which is the root of the graph summarization runs.
        -d* : a dataset name. Can be multi-valued.
        -a* : an algorithm name. Can be multi-valued.
        -b* : where the hadoop bin is.
        -c* : The YAML configuration file of the summary cascade.
        -j* : the path to the analytics-benchmark jar file.
        -D  : A parameter to pass to Hadoop. Can be multi-valued.
EOF
}

###
### Parse the options
###
if [ $# == 0 ]; then
  _usage
  exit 1
fi
HAS_I=1
HAS_C=1
HAS_D=1
HAS_A=1
HAS_B=1
HAS_J=1
while getopts ":hi:o:b:c:j:D:a:d:" opt; do
  case $opt in
    h)
        _usage
        exit 1
    ;;
    D)
        D_OPTIONS="-D$OPTARG $D_OPTIONS"
    ;;
    j)
        HAS_J=0
        JAR="$OPTARG"
        echo "Path to jar: $OPTARG"
    ;;
    i)
        HAS_I=0
        INPUT="$OPTARG"
        echo "Adding the input directory: $OPTARG"
    ;;
    d)
        HAS_D=0
        DATASETS="$DATASETS $OPTARG"
        echo "Adding the dataset: $OPTARG"
    ;;
    a)
        HAS_A=0
        ALGORITHMS="$ALGORITHMS $OPTARG"
        echo "Adding the algorithm: $OPTARG"
    ;;
    b)
        HAS_B=0
        if [ ! -f "$OPTARG" ]; then
            echo "Missing path to the hadoop bin (-b option)"
            _usage
            exit 1
        fi
        echo "Setting the hadoop bin to: $OPTARG"
        HADOOP_BIN="$OPTARG"
    ;;
    c)
        HAS_C=0
        if [ ! -f "$OPTARG" ]; then
            echo "Missing path to the yaml configuration file (-c option)"
            _usage
            exit 1
        fi
        echo "Using the YAML configuration file at: $OPTARG"
        CONFIG_YAML="$OPTARG"
    ;;
    \?)
        echo "Invalid option: -$OPTARG"
        _usage
        exit 1
    ;;
    :)
        echo "Option -$OPTARG requires an argument." >&2
        _usage
        exit 1
    ;;
  esac
done

# Check that all mandatory parameters are here
_req_option $HAS_J j
_req_option $HAS_I i
_req_option $HAS_B b
_req_option $HAS_D d
_req_option $HAS_A a
_req_option $HAS_C c

###
### Start the experiment
###

echo "Using the following hadoop parameters [$D_OPTIONS]"

rm -f run-connectivity.log

for dataset in $DATASETS; do
    for algo in $ALGORITHMS; do
        # Get the gold standard output for this dataset
        gold_path="/$($HADOOP_BIN fs $HADOOP_CLUSTER -ls $INPUT/$dataset/*/$GOLD_STANDARD* | cut -f 2- -d '/' | sort -d | tail -n 1)"
        gold_path=$(dirname $gold_path)
        echo "Taking [$gold_path] as Gold Standard"

        # Take the latest results
        path="/$($HADOOP_BIN fs $HADOOP_CLUSTER -ls $INPUT/$dataset/*/$algo-run* | cut -f 2- -d '/' | sort -d | tail -n 1)"
        path=$(dirname $path)
        if [[ $path = "/" ]]; then
            echo "Skipping! there is no summary for the given [$dataset] - [$algo] pair"
            continue
        fi
        echo "Running the connectivity precision on [$path]"
        # Check that the LinksetsCLI has been run on this folder
        ## it creates 2 outputs: $path/precision/eval-linkset and $path/precision/gold-eval-table
        if !($HADOOP_BIN fs $HADOOP_CLUSTER -test -d $path/precision/eval-linkset > /dev/null 2>&1) ||
            !($HADOOP_BIN fs $HADOOP_CLUSTER -test -d $path/precision/gold-eval-table > /dev/null 2>&1); then
            echo "Run LinksetsCLI"
            $HADOOP_BIN jar $JAR org.sindice.analytics.benchmark.cascading.precision.LinksetsCLI \
                    $HADOOP_CLUSTER $D_OPTIONS \
                    --cascade-config $CONFIG_YAML \
                    --input $gold_path,$path \
                    --output $path/precision
        fi
        # Run the Connectivity precision
        if !($HADOOP_BIN fs $HADOOP_CLUSTER -test -d $path/precision/tf-positive); then
            echo "Run ConnectivityAssemblyCLI"
            $HADOOP_BIN jar $JAR org.sindice.analytics.benchmark.cascading.precision.connectivity.ConnectivityAssemblyCLI \
                        $HADOOP_CLUSTER $D_OPTIONS \
                        --cascade-config $CONFIG_YAML \
                        --input $path/precision \
                        --output $path/precision
        fi
    done
done >> run-connectivity.log 2>&1
