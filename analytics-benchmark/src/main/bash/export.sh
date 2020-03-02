#! /bin/bash

########################################################################
## This script runs the schema precision evaluation over the given    ##
## list of datasets and graph summarization algorithms.               ##
##                                                                    ##
## This script relies on a specific directory structure about the     ##
## graph summarization outputs. Given a root folder named <ROOT>, it  ##
## expects to find the following layout:                              ##
##     <DATASET_NAME>/<DATE>/<ALGORITHM>-run-<X>                      ##
## where we have in order the name of the dataset, the date when the  ##
## Hadoop job started with format yyyy-mm-dd, the name of the graph   ##
## summarization algorithm, and <X> identifies multiple runs of the   ##
## Hadoop job on that date.                                           ##
##                                                                    ##
## The resuls of an algorithm on a dataset are exported in a local    ##
## folder, which hierarchy is the same as above, except that there is ##
## no <DATE> intermediate folder. This local folder is used only as a ##
## place for exporting the results, but not to save those exports.    ##
## The exported results are available at the root of the local folder.##
##                                                                    ##
## The following set of results exporters are used:                   ##
## - sindice.viz.connectivity.ConnectivityExporter                    ##
## - sindice.viz.schema.SchemaExporter                                ##
## - sindice.viz.machineUsage.MachineUsageExporter                    ##
## where "sindice" package is a shortcut for                          ##
## org.sindice.analytics.benchmark.cascading                          ##
########################################################################

source common.sh

###
### Variables
###
WORKDIR=$(dirname $0)
INPUT="/user/stecam/summary" # the folder on HDFS with the precision results.

###
### Options variables
###
FOLDER="The local folder, where to export the data"
DATASETS="" # The list of dataset to process
ALGORITHMS="" # The list of algorithms to evaluate
COUNTERS="" # The path to the saved Hadoop counters
N=5

HADOOP_BIN="The path to the hadoop bin script"
CONFIG_YAML="The YAML configuration file of the summary cascade"
JAR="The path to the JAR file"
D_OPTIONS="" # list of hadoop parameters

## Print the usage description of the script
function _usage() {
cat <<EOF
    Welcome to the Data Graph Summary benchmark CLI - Exporter.
    Here are the options (options with * are mandatory):
        -h  : print this help.
        -i* : the local folder, where to export the data.
        -d* : a dataset name. Can be multi-valued.
        -a* : an algorithm name. Can be multi-valued.
        -b* : where the hadoop bin is.
        -j* : the path to the analytics-benchmark jar file.
        -s  : the path to the stored hadoop counters.
        -n  : when option -s is used, this indicates how many runs has to be taken (Default: 5).
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
HAS_D=1
HAS_A=1
HAS_B=1
HAS_J=1
while getopts ":hi:b:j:a:d:s:n:" opt; do
  case $opt in
    h)
        _usage
        exit 1
    ;;
    n)
        N="$OPTARG"
        echo "Taking the measurements of $N runs (Machine Usage export)"
    ;;
    j)
        HAS_J=0
        JAR="$OPTARG"
        echo "Path to jar: $OPTARG"
    ;;
    i)
        HAS_I=0
        FOLDER="$OPTARG"
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
    s)
        COUNTERS="$OPTARG"
        if [ ! -d "$OPTARG" ]; then
            echo "Missing path to the job counters directory (-s option)"
            _usage
            exit 1
        fi
      echo "Setting the counters directory to: $COUNTERS"
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

# Export the results

FOLDER=$(readlink -f $FOLDER)

# Delete previous folder ?
if [[ -d $FOLDER ]]; then
    delete="none"
    while [[ $delete != "y" ]] && [[ $delete != "n" ]]; do
        echo -n "Delete previous folder [$FOLDER] ? (y/n) "
        read delete;
    done
    if [[ $delete = "y" ]]; then
        rm -rf $FOLDER
    fi
fi

for dataset in $DATASETS; do
    for algo in $ALGORITHMS; do
        echo -e "${COLOR_GREEN}Processing [$dataset] - [$algo]${COLOR_END}"
        mkdir -p $FOLDER/$dataset/$algo

        # Connectivity path - the latest available
        conn_path="/$($HADOOP_BIN fs $HADOOP_CLUSTER -ls $INPUT/$dataset/*/$algo-run*/precision/tf-positive | cut -f 2- -d '/' | sort -d | tail -n 1)"
        conn_path=$(dirname $conn_path)
        if [[ $conn_path != "/" ]]; then
            echo -e "\tGetting connectivity precision from [$conn_path]"
            # Connectivity
            $HADOOP_BIN fs $HADOOP_CLUSTER -get $conn_path $FOLDER/$dataset/$algo
        fi

        # Schema path - the latest available
        schema_path="/$($HADOOP_BIN fs $HADOOP_CLUSTER -ls $INPUT/$dataset/*/$algo-run*/precision/schema | cut -f 2- -d '/' | sort -d | tail -n 1)"
        schema_path=$(dirname $schema_path)
        if [[ $schema_path != "/" ]]; then
            echo -e "\tGetting Schema precision from [$schema_path]"
            # Schema
            $HADOOP_BIN fs $HADOOP_CLUSTER -get $schema_path $FOLDER/$dataset/$algo
        fi

        # Machine Usage - take latest stats
        i=0
        for file in $(ls $COUNTERS/$dataset/*/${algo}-run-*/Data-Graph-Summary/relations-graph/global.yaml | sort -d | tail -n $N); do
            i=$((i+1))
            ln -s $file $FOLDER/$dataset/$algo/run$i-global.yaml
            echo -e "\tGetting Machine Usage from [$file/Data-Graph-Summary/relations-graph/global.yaml]"
        done
        if [[ $i -ne $N ]]; then
            echo -e "${COLOR_RED}Got $i runs for [$dataset/.*/${algo}] in [$COUNTERS] ${COLOR_END}"
        fi

        echo
    done
done

java -cp $JAR org.sindice.analytics.benchmark.cascading.viz.ExporterCLI \
                --input $FOLDER \
                --round 4 \
                --formatter-type LATEX \
                --exporter org.sindice.analytics.benchmark.cascading.viz.connectivity.ConnectivityExporter,org.sindice.analytics.benchmark.cascading.viz.schema.SchemaExporter,org.sindice.analytics.benchmark.cascading.viz.machineUsage.MachineUsageExporter
