#!/bin/bash

dir=octocat-2011-09-24
#dir="tub78-2011-09-21"

log="$dir/LOG.txt"

cmd="python krwlr.py --max_download 10 --max_distance 2 --sleep 500000 --download_dir $dir"
#cmd="python krwlr.py --max_download 500 --max_distance 2 --sleep 500000 --download_dir $dir"

echo "echo $cmd >> $log"
echo "$cmd 2>&1| tee -a $log"

