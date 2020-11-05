#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./4_coda_add.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Uploads coded messages datasets from '<data-root>/Outputs/Coda Files' to Coda"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3

./checkout_coda_v2.sh "$CODA_V2_ROOT"

DATASETS=(
    "UNICEF-COVID19-KE_s01e01"
    "UNICEF-COVID19-KE_s01e02"
    "UNICEF-COVID19-KE_s01e03"
    "UNICEF-COVID19-KE_s01e04"
    "UNICEF-COVID19-KE_s01e05"

    "UNICEF-COVID19-KE_age"
    "UNICEF-COVID19-KE_gender"
    "UNICEF-COVID19-KE_location"
)

cd "$CODA_V2_ROOT/data_tools"
git checkout "e895887b3abceb63bab672a262d5c1dd73dcad92"  # (master which supports incremental get)

for DATASET in ${DATASETS[@]}
do
    echo "Pushing messages data to ${DATASET}..."

    pipenv run python add.py "$AUTH" "${DATASET}" messages "$DATA_ROOT/Outputs/Coda Files/$DATASET.json"
done
