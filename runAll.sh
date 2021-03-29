#!/bin/sh

# A quick little script to run the entire process of scrubbing and fuzzing


if [ $# -eq 0 ] 
then
  echo "Usage: runAll.sh <version_number> <input_file> "
  echo "E.g., runall.sh 5_1 radiocarbon_dates_MUSHv5_1.csv"
  exit 1
fi

version=$1
mushFile=$2

scrubbed="radiocarbon_dates_scrubbedv$1.csv"
graveyard="GRAVEYARD_v$1.csv"
fuzzed="radiocarbon_dates_scrubbed_and_fuzzed_v$1.csv"

python scrub.py $mushFile radiocarbon_dates_scrubbed_v$1.csv GRAVEYARD_v$1.csv
python centroids/fuzz.py $scrubbed $fuzzed

cp $scrubbed out/$scrubbed
cp $graveyard out/$graveyard
cp $fuzzed out/$fuzzed
