#! /bin/csh
#
# use gcc   (v2.5.2) for signed/unsigned argument mismatch checking
# use gcc-2.7.2  for 64 bit int support
echo "${1}:"

setenv JAVA_HOME /gcm/where/jdk/1.4.1.02/sparc.Solaris

pushd ../../../../..
setenv base $cwd
popd
gcc -c $* -o /dev/null -O2 -mv8 \
 -DNATIVE -D_REENTRANT -DFLG_LINT_SWITCHES \
 -Wstrict-prototypes -Wmissing-prototypes -Wnested-externs \
 -Winline -Wall -Wpointer-arith -Wwrite-strings -Wshadow \
 -I. -I${base}/src/generated -I{$base}/build-artifacts \
 -I${JAVA_HOME}/include -I${JAVA_HOME}/include/solaris \
|& egrep -v -e 'warning.*format,|: In function' \
| egrep -v -e 'jni.h: In method|warning.*varargs function cannot be inline' 

