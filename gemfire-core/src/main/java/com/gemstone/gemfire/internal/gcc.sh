#! /bin/csh
#

if ( $1 == "full" ) then
 # echo "full"
  gccimpl.sh $2 -DFLG_DEBUG 
else
 # echo "filtered"
 gccimpl.sh $* -DFLG_DEBUG \
 |& egrep -v -e ': In function' \
 | egrep -v -e 'jni.h: In method|warning.*varargs function cannot be inline' \
 | egrep -v -e '\.hpp.*will never be executed' | less -iX
endif
