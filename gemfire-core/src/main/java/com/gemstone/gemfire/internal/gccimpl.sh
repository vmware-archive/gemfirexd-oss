#! /bin/csh
#
# use gcc   (v2.5.2) for signed/unsigned argument mismatch checking
# use gcc-2.7.2  for 64 bit int support
echo "${1}:"

pushd ../../../../..
setenv base $cwd
popd

if ( $machine == "10") then
  setenv JAVA_HOME /gcm/where/jdk/1.4.1.02/sparc.Solaris
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

# echo "JINCL $JINCL"
# echo "builddir $builddir"
g++ -c $* -o /dev/null -O2  \
 -DGEMFIRE_VERSION='"fakeVersion"' \
 -DNATIVE -D_REENTRANT -DFLG_LINT_SWITCHES \
 -Wuninitialized \
 -Wunreachable-code \
 -Wstrict-prototypes -Wmissing-prototypes -Wnested-externs \
 -Wmissing-declarations \
 -Winline -Wall -Wpointer-arith -Wwrite-strings -Wshadow \
 -I. -I${builddir}/src/generated \
 ${JINCL} ${LICINCL} \


# GCC 2.0 switches...
# includes="-I/usr/local/${machine}lib/gcc-include"
# No  -Waggregate-return; this is needed for dbm
# -pedantic -Wshadow
# extraSwitches="-mv8 -Wstrict-prototypes -Wconversion\
#   -Wmissing-prototypes -Wnested-externs -Winline \
#   -Wall -Wpointer-arith -Wwrite-strings -Wshadow"
# use either gcc-2.7.2 or gcc (latter is old 2.5 which is stricter on
#  signed/unsigned parameter checks.

