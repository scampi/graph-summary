#! /bin/bash

########################################################################
###                                                                  ###
###  This script is configured to launch the summary computation     ###
###  over N-Triples, applying the SINGLE_TYPE clustering algorithm.  ###
###  This scenario considers the existence of a single dataset.      ###
###  Therefore, we disable the check for entity authority.           ###
###                                                                  ###
########################################################################

function _usage() {
cat <<EOF
Welcome to the Data Graph Summary CLI.
    Here are the options (options with * are mandatory):
        -i* : input directory on HDFS.
        -o* : output directory on local.
        -b* : where the hadoop bin is.
        -u* : the dataset URI.
EOF
}

###
### Variables
###
HADOOP_BIN="The path to the hadoop bin script"
INPUT="input directory on HDFS with the dataset, in N-Triples format"
OUTPUT="The output directory on HDFS (GZip compressed, in N-Quads format)"
URI="the dataset URI"

JAR="hadoop-summary-assembly.jar"
CONFIG_YAML="config.yaml"

###
### Parse the options
###
if [ $# == 0 ]; then
    _usage
    exit 1
fi

# mandatory options
HAS_I=1
HAS_O=1
HAS_B=1
HAS_U=1
while getopts ":hi:o:b:u:" opt; do
    case $opt in
    h)
        _usage
        exit 1
    ;;
    i)
        HAS_I=0
        INPUT="$OPTARG"
        # Remove leading slash
        if [ ${INPUT:${#INPUT}-1} = "/" ]; then
            INPUT=${INPUT:0:${#INPUT}-1}
        fi
        echo -e "\e[00;32mSetting the input directory to:\e[00m $INPUT"
    ;;
    o)
        HAS_O=0
        OUTPUT="$OPTARG"
        echo -e "\e[00;32mSetting the output directory to:\e[00m $OUTPUT"
    ;;
    u)
        HAS_U=0
        URI="$OPTARG"
        echo -e "\e[00;32mSetting the dataset URI to:\e[00m $URI"
    ;;
    b)
        HAS_B=0
        if [ ! -f "$OPTARG" ]; then
            echo -e "\e[00;31mMissing path to the hadoop bin (-b option)\e[00m"
            _usage
        exit 1
        fi
        echo -e "\e[00;32mSetting the hadoop bin to:\e[00m $OPTARG"
        HADOOP_BIN="$OPTARG"
    ;;
    \?)
        echo -e "\e[00;31mInvalid option: -$OPTARG\e[00m"
        _usage
        exit 1
    ;;
    :)
        echo -e "\e[00;31mOption -$OPTARG requires an argument.\e[00m" >&2
        _usage
        exit 1
    ;;
    esac
done

# Check that all mandatory parameters are here
if [[ $HAS_I -eq 1 ]]; then
    echo -e "\e[00;31mMissing parameter -i\e[00m"
    _usage
    exit 1
fi
if [[ $HAS_O -eq 1 ]]; then
    echo -e "\e[00;31mMissing parameter -o\e[00m"
    _usage
    exit 1
fi
if [[ $HAS_B -eq 1 ]]; then
    echo -e "\e[00;31mMissing parameter -b\e[00m"
    _usage
    exit 1
fi
if [[ $HAS_U -eq 1 ]]; then
    echo -e "\e[00;31mMissing parameter -u\e[00m"
    _usage
    exit 1
fi

# Run the summary
echo -e "\e[00;32mRunning the Data Graph Summary computation\e[00m"
if $HADOOP_BIN fs -test -d $INPUT-output; then
    $HADOOP_BIN fs -rmr $INPUT-output
fi
$HADOOP_BIN jar $JAR org.sindice.graphsummary.cascading.DataGraphSummaryCascadeCLI      \
                     -Ddocument-format=NTRIPLES                                         \
                     -Ddataset-uri=$URI                                                 \
                     -Dcheck-auth-type=false                                            \
                     --input $INPUT                                                     \
                     --output $INPUT-output                                             \
                     --cascade-config $CONFIG_YAML                                      \
                     --input-format TEXTLINE                                            \
                     --cluster-algorithm SINGLE_TYPE

if [[ $? -ne 0 ]]; then
    echo -e "\e[00;31mFailed to execute the summary\e[00m"
    exit 1
fi

# Copy the RDF dumps
mkdir $OUTPUT
$HADOOP_BIN fs -get $INPUT-output/relations-graph-rdf/part* $OUTPUT
cd $OUTPUT
rename 's/part/relations/' part*

cd -
$HADOOP_BIN fs -get $INPUT-output/metadata-graph-rdf/part* $OUTPUT
cd $OUTPUT
rename 's/part/metadata/' part*

echo -e "\e[00;32mDone. Check $OUTPUT\e[00m"
