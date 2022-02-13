#!/bin/bash

echo "**************************************************************************"
echo "You are configurating EPL Infra"
echo "**************************************************************************"

#Deply Infra
terraform plan
terraform apply

#Transform JSON files to NDJASON files
echo "Transform JSON files to NDJASON files"
cat ../../data/season-0910_json.json | jq -c '.[]' > ../../data/NDseason-0910_json.json
cat ../../data/season-1011_json.json | jq -c '.[]' > ../../data/NDseason-1011_json.json
cat ../../data/season-1112_json.json | jq -c '.[]' > ../../data/NDseason-1112_json.json
cat ../../data/season-1213_json.json | jq -c '.[]' > ../../data/NDseason-1213_json.json
cat ../../data/season-1314_json.json | jq -c '.[]' > ../../data/NDseason-1314_json.json
cat ../../data/season-1415_json.json | jq -c '.[]' > ../../data/NDseason-1415_json.json
cat ../../data/season-1516_json.json | jq -c '.[]' > ../../data/NDseason-1516_json.json
cat ../../data/season-1617_json.json | jq -c '.[]' > ../../data/NDseason-1617_json.json
cat ../../data/season-1718_json.json | jq -c '.[]' > ../../data/NDseason-1718_json.json
cat ../../data/season-1819_json.json | jq -c '.[]' > ../../data/NDseason-1819_json.json

#Send files to bucket
echo "Send files to bucket"
export bucket_landing="gs://epl-landing-file-5886940c92ed"

gsutil cp ../../data/NDseason-0910_json.json "$bucket_landing/NDseason-0910_json.json"
gsutil cp ../../data/NDseason-1011_json.json "$bucket_landing/NDseason-1011_json.json"
gsutil cp ../../data/NDseason-1112_json.json "$bucket_landing/NDseason-1112_json.json"
gsutil cp ../../data/NDseason-1213_json.json "$bucket_landing/NDseason-1213_json.json"
gsutil cp ../../data/NDseason-1314_json.json "$bucket_landing/NDseason-1314_json.json"
gsutil cp ../../data/NDseason-1415_json.json "$bucket_landing/NDseason-1415_json.json"
gsutil cp ../../data/NDseason-1516_json.json "$bucket_landing/NDseason-1516_json.json"
gsutil cp ../../data/NDseason-1617_json.json "$bucket_landing/NDseason-1617_json.json"
gsutil cp ../../data/NDseason-1718_json.json "$bucket_landing/NDseason-1718_json.json"
gsutil cp ../../data/NDseason-1819_json.json "$bucket_landing/NDseason-1819_json.json"
gsutil cp ../../src/bigquery/schema-epl.json "$bucket_landing/schema-epl.json"