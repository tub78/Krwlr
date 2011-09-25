#!/bin/bash

log="tub78-2011-09-21/LOG-2011-09-21.txt"

cmd="python crawler.py --max_download 500 --max_distance 2 --sleep 500000 --download_dir tub78-2011-09-21"

echo "echo $cmd >> $log"
echo "$cmd 2>&1| tee -a $log"

