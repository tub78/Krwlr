#!/bin/bash

#dir=octocat-$(date "+%Y-%m-%d")

dir=octocat-2011-09-26
#dir=octocat-2011-09-24
#dir="tub78-2011-09-21"

log="$dir/LOG.txt"

cmd="python github_krwlr.py --max_download 10 --max_distance 2 --sleep 500000 --download_dir $dir"
#cmd="python github_krwlr.py --max_download 500 --max_distance 2 --sleep 500000 --download_dir $dir"

#cmd="python krwlr.py --max_download 500 --max_distance 2 --sleep 500000 --download_dir $dir"


echo "echo $cmd >> $log"
echo "$cmd 2>&1| tee -a $log"

