#!/bin/bash

startdate=2019-07-15
enddate=2019-07-31

sDateTs=`date -j -f "%Y-%m-%d" $startdate "+%s"`
eDateTs=`date -j -f "%Y-%m-%d" $enddate "+%s"`
dateTs=$sDateTs
offset=86400

##/cf-analytics-logs-elastic-testing
##/cf-analytics-logs-elastic

while [ "$dateTs" -le "$eDateTs" ]
do
  date=`date -j -f "%s" $dateTs "+%Y%m%d"`
  echo "s3cmd modify --recursive --add-header="touched:touched" s3:///cf-analytics-logs-elastic/$date"
  (s3cmd modify --recursive --add-header="touched:touched" s3:///cf-analytics-logs-elastic/$date) &
  dateTs=$(($dateTs+$offset))
done

#for i in $dates; do 
#    echo "s3cmd modify --recursive --add-header="touched:touched" s3:///camiliame-2/$i"
#    (s3cmd modify --recursive --add-header="touched:touched" s3:///camiliame-2/$i) &
#done
