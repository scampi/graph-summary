#! /bin/bash

########################################################################
## This script runs the summary over a set of datasets, using the     ##
## defined set of clustering algorithms $CLUSTERING_ALGORITHM. For    ##
## each dataset and each clustering algorithms, the computation is    ##
## executed N times.                                                  ##
##                                                                    ##
## The results are stored in HDFS, with the following file hierarchy: ##
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
CLUSTERING_ALGORITHM=( F_BISIMULATION TYPES PROPERTIES TYPES_PROPERTIES SINGLE_TYPE IO_PROPERTIES IO_PROPERTIES_CLASSES ) # NAMESPACE IO_NAMESPACE DATATYPE )

WORKDIR=$(dirname $0)
HADOOP_BIN="The path to the hadoop bin script"
NB_INPUTS="0" # Number of datasets
INPUTS="Array of input directory on HDFS with the dataset"
OUTPUT="The output directory on HDFS"
JAR="The hadoop summary assembly jar"
FORMAT="CASCADING_SEQUENCE_FILE"
CONFIG_YAML="The YAML configuration file of the summary cascade"
COUNTERS="$WORKDIR/../../../results"
N_RUNS=5
D_OPTIONS="" # list of hadoop parameters

function _usage() {
cat <<EOF
    Welcome to the Data Graph Summary benchmark CLI.
    Here are the options:
        -h : print this help.
        -i : input directory on HDFS (mandatory). Can be multi-valued.
        -o : output directory on HDFS (mandatory).
        -n : number of times to repeat the computation.
             Default: 5
        -b : where the hadoop bin is (mandatory).
        -f : format of the input data (CASCADING_SEQUENCE_FILE, HADOOP_SEQUENCE_FILE, TEXTLINE).
             Default: CASCADING_SEQUENCE_FILE
        -c : The YAML configuration file of the summary cascade (mandatory).
        -s : the directory (local) where to store the jobs counters.
             Default: $WORKDIR/../../../results
        -j : the path to the jar file (mandatory).
        -D : A parameter to pass to Hadoop. Can be multi-valued.
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
HAS_O=1
HAS_B=1
HAS_C=1
HAS_J=1
while getopts ":hi:o:n:b:f:c:s:j:D:" opt; do
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
        NB_INPUTS=$((NB_INPUTS + 1))
        INPUTS[$((NB_INPUTS - 1))]="$OPTARG"
        echo "Adding the input directory: $OPTARG"
    ;;
    o)
        HAS_O=0
        OUTPUT="$OPTARG"
        echo "Setting the output directory to: $OUTPUT"
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
    n)
        N_RUNS="$OPTARG"
        echo "Executing the computation $N_RUNS times"
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
    f)
        if [[ $OPTARG != "CASCADING_SEQUENCE_FILE" ]] && [[ $OPTARG != "HADOOP_SEQUENCE_FILE" ]] && [[ $OPTARG != "TEXTLINE" ]]; then
            echo "Format must be either CASCADING_SEQUENCE_FILE, HADOOP_SEQUENCE_FILE, TEXTLINE"
           _usage
            exit 1
        fi
        FORMAT=$OPTARG
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
_req_option $HAS_O o
_req_option $HAS_B b
_req_option $HAS_C c

###
### Start the experiment
###

DATE=$(date +%Y-%m-%d)
echo "Exporting the Jobs counters into: $COUNTERS/<dataset>/$DATE"
echo "Using the following hadoop parameters [$D_OPTIONS]"

rand=$(echo $RANDOM)
rm run-${DATE}_$rand.sh
rm run-${DATE}_$rand.log

for i in $(seq 1 $N_RUNS); do ### Perform n runs
  echo -n "echo \"Run #$i\" >> run-${DATE}_$rand.log;"

  for j in $(seq 0 $((NB_INPUTS - 1))); do ### For each input

    for cluster in ${CLUSTERING_ALGORITHM[*]}; do ### For each clustering algorithm

        DATASET_NAME=$(echo ${INPUTS[$j]} | sed 's/\/$//' | awk -F '/' '{print $NF}')
        echo -n "echo \"Executing the summary on $DATASET_NAME, with the clustering $cluster\" >> run-${DATE}_$rand.log;"

        # Store the Hadoop job counters in $COUNTERS/$DATASET_NAME/$DATE/${cluster}-run-$i
        mkdir -p $COUNTERS/$DATASET_NAME/$DATE/${cluster}-run-$i

        # The dataset_name is taken as the default domain (Only needed when dealing with N-Triples
        echo -n "$HADOOP_BIN jar $JAR org.sindice.graphsummary.cascading.DataGraphSummaryCascadeCLI $HADOOP_CLUSTER $D_OPTIONS --input ${INPUTS[$j]} --output ${OUTPUT}/${DATASET_NAME}/$DATE/${cluster}-run-$i --cascade-config $CONFIG_YAML --input-format $FORMAT --counters $COUNTERS/$DATASET_NAME/$DATE/${cluster}-run-$i --cluster-algorithm $cluster >> run-${DATE}_$rand.log 2>&1;"

        echo "if [[ \$? -ne 0 ]]; then echo \"Failed to execute the summary\" >> run-${DATE}_$rand.log; exit 1; fi;"

    done
  done
done | shuf > run-${DATE}_$rand.sh

bash run-${DATE}_$rand.sh
