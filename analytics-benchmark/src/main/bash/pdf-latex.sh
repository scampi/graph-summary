#! /bin/bash

################################################################################
## Create PDF files from latex files for all results of the benchmark         ##
## exported into a .tex file.                                                 ##
################################################################################

WORKDIR=$(dirname $0)
NB_INPUTS="0" # Number of datasets
INPUTS="The input directory with the benchmark counters"

function _usage() {
cat <<EOF
	Welcome to the Data Graph Summary benchmark CLI - LATEX to PDF script.
	Here are the options:
		-i : the input directory with the benchmark counters.
		     Can be multivalued.
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
while getopts ":hi:" opt; do
  case $opt in
  	h)
		  _usage
		  exit 1
	    ;;
    i)
      HAS_I=0
      NB_INPUTS=$((NB_INPUTS + 1))
      INPUTS[$((NB_INPUTS - 1))]="$OPTARG"
      echo "Adding the input directory: $OPTARG"
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
if [[ $HAS_I -eq 1 ]]; then
  echo "Missing parameter -i"
  _usage
  exit 1
fi

# Create the PDF files
for input in ${INPUTS[*]}; do ### For each input
  echo "Processing $input"
  TEX_FILES="$(find $input -iname "*.tex")"
  IFS=$'\n'
  for tex in $TEX_FILES; do
    base=$(dirname $tex)
    filename=$(echo $tex | awk -F'/' '{print $NF}')
    echo "Processing $tex"
    cd $base && pdflatex $filename
    cd $WORKDIR
  done
done

