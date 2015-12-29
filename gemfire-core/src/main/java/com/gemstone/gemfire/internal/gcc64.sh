#! /bin/tcsh
#
# use gcc   (v2.5.2) for signed/unsigned argument mismatch checking
# use gcc-2.7.2  for 64 bit int support

setenv pushdsilent 1
pushd ../../../../..
setenv base $cwd
popd

if ( $machine == "10") then
  setenv JAVA_HOME /gcm/where/jdk/1.4.1.02/sparc.Solaris64
  setenv JINCL "-DSOLARIS=1 -I${JAVA_HOME}/include -I${JAVA_HOME}/include/solaris "
  setenv LICINCL "-I/gcm/where/sentinel/7.2.0/shared/include  "
  setenv Plat sol
  setenv builddir ${base}/build-artifacts/${Plat}
else
  setenv Plat linux
  setenv JAVA_HOME /gcm/where/jdk/1.4.1.02/x86.linux
  setenv JINCL "-I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux "
  setenv LICINCL "-I/gcm/where/sentinel/7.2.0_linux/shared/include  "
  setenv builddir ${base}/build-linux
endif


echo "${1}:"
# -Wconversion

/export/localnew/sparc.Solaris/gcc32/bin/g++ \
  -c $1 -o /dev/null -O2 -m64 -mptr64  -D_LP64 \
 -DNATIVE -D_REENTRANT $2 -DFLG_LINT_SWITCHES \
 -DGEMFIRE_VERSION='"fakeVersion"' -DLICENSE_VERSION='"junk"' \
 -DGEMFIRE_BUILDID='"999"' -fcheck-new \
 -fno-default-inline -finline-limit=0 \
 -Wstrict-prototypes -Wmissing-prototypes -Wnested-externs \
 -Wmissing-declarations \
 -Winline -Wall -Wpointer-arith -Wwrite-strings -Wconversion \
 -Wshadow -Wno-unused-function \
 -I. -I${builddir}/src/generated \
 ${JINCL} ${LICINCL} \


# |& egrep -v -e ': In function|In static member function|defined but not used' \
# | egrep -v -e 'jni.h: In method|warning.*varargs function cannot be inline' \
# | egrep -v -e 'candidates are:' 

# -Wcast-align -pedantic

exit
#  egrep -v -e 'warning:.*operator new. must not return NULL|-fcheck-new|candidates are:' \

