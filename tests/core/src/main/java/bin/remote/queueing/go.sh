#! /bin/sh
here=/export/shared_build/users/jpenney/run_bensley
#debug=-d
debug="-w"

exec 0</dev/null

# nohup \
perl $debug $here/runqueue.pl \
  log=$here/runqueue.log \
  commandFile=$here/bensley.txt \
  pauseFile=$here/pause.txt 
#    2>&1 | tee >>go.log
