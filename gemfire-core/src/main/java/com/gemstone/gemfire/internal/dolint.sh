#! /bin/csh

rm -f lint.log

dolintimpl.sh >& lint.log
dolintimpl.sh -DFLG_DEBUG >>& lint.log

grep warning lint.log |egrep -v -e 'operator new should throw an exception|can.t inline call|operator new should throw an exception|called from here|defined but not used'
