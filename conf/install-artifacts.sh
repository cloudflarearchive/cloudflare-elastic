#!/bin/bash

function usage {
      echo ""
      echo "usage: install-artifacts.sh -u <username> -p <password> -e <endpoint> -c <cacert>"
      echo "    -u  elasticsearch username"
      echo "    -p  elasticsearch password"
      echo "    -e  elasticsearch endpoint, e.g. https://localhost:9200"
      echo "    -c  cacert file (optional)"
      echo ""
}

username=""
password=""
endpoint=""
cacert=""
auth=""

while getopts ":u:p:e:c:h" opt; do
  case $opt in
    u)
      username=$OPTARG
      ;;
    p)
      password=$OPTARG 
      ;;
    e)
      endpoint=$OPTARG 
      ;;
    c)
      cacert="--cacert $OPTARG"
      ;;
    h)
      usage
      exit 1
      ;;
    \?)
      usage
      exit 1
      ;;
  esac
done


if [[ "$username" && "$password" ]];
then
    auth="-u $username:$password"
fi

if [ -z "$endpoint" ];
then
    usage
    exit 1
fi


echo ""
echo "Installing ingest pipeline to $endpoint for daily indices"
curl -X PUT $auth $cacert "$endpoint/_ingest/pipeline/cloudflare-pipeline-daily" -H 'Content-Type: application/json' -d @cloudflare-ingest-pipeline-daily.json
echo ""
echo "Installing ingest pipeline to $endpoint for weekly indices"
curl -X PUT $auth $cacert "$endpoint/_ingest/pipeline/cloudflare-pipeline-weekly" -H 'Content-Type: application/json' -d @cloudflare-ingest-pipeline-weekly.json
echo ""
echo "Installing index template to $endpoint"
curl -X PUT $auth $cacert "$endpoint/_template/cloudflare" -H 'Content-Type: application/json' -d @cloudflare-index-template.json
echo ""
