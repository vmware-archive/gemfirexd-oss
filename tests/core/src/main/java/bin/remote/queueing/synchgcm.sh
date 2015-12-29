#! /bin/sh
here=/export/shared_build/users/jpenney/run_bensley

# Magic script to synch /gcm, per Mike Culbertson's directions

cd $here
ssh bensf1 "cd /bensf1; sudo rsync -azuvHl delta:/vol ."
