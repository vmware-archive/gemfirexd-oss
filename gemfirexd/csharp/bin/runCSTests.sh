#!/bin/bash

ISWIN=
if uname | grep '^CYGWIN' >/dev/null 2>/dev/null; then
  ISWIN=1
fi
export ISWIN

getPath()
{
  if [ "${ISWIN}" = "1" ]; then
    cygpath "$1"
  else
    echo "$1"
  fi
}

getOSPath()
{
  if [ "${ISWIN}" = "1" ]; then
    cygpath -m "$1"
  else
    echo "$1"
  fi
}

scriptLocation="`getPath "$0"`"
scriptDir="`dirname "${scriptLocation}"`"
scriptName="`basename "${scriptLocation}"`"

if [ -r "${scriptDir}/runCSFunctions.sh" ]; then
  source "${scriptDir}/runCSFunctions.sh"
else
  echo "Could not read the runCSFunctions.sh script."
  exit 1
fi

if [ -z "${NUNIT}" ]; then
  echo NUNIT not set
  exit 1
fi
PATH="${PATH}:`getPath "${NUNIT}/bin"`"

NUNIT_CONSOLE="${NUNIT}/bin/nunit-console.exe"
NUNIT_FLAGS="-labels"
UNITTESTS_DLL="Pivotal.Data.GemFireXD.Tests.dll"
RESULT_XML="TestResult.xml"


getResultsDir()
{
  echo "${TESTRES}/csharp/$1"
}

getNunitFlag()
{
  # not using RESULT_TYPE currently
  echo
}

getFailures()
{
  sed -n 's/^[ \t]*<test-case[ \t]\+name[ \t]*=[ \t]*"\([^"]*\)\.[^"]*"[^>]*success[ \t]*=[ \t]*"False"[^>]*>.*$/\1/p' "$1" 2>/dev/null
}

runTests()
{
  echo "The logging level is ${UNIT_LOGLEVEL}."
  echo "The server logging level is ${GFXD_LOGLEVEL}."
  echo "The server security logging level is ${GFXD_SECLOGLEVEL}."
  echo
  ORIGPWD="`pwd`"
  RESULT_TYPE="$1"
  shift
  RESULTS_DIR="`getResultsDir "${RESULT_TYPE}"`"
  NUNIT_RUN_FLAG="`getNunitFlag "${RESULT_TYPE}"`"
  OUTDIR=
  rm -rf "${RESULTS_DIR}"
  mkdir -p "${RESULTS_DIR}"
  if [ -n "${GFXDADOOUTDIR}" ]; then
    OUTDIR="${RESULTS_DIR}/${GFXDADOOUTDIR}"
    mkdir -p "${OUTDIR}"
    cp -dR "${scriptDir}/tests" "${OUTDIR}"
    GFXDADOOUTDIR="`getOSPath "${OUTDIR}"`"
    export GFXDADOOUTDIR
  fi
  cd "${scriptDir}/${RESULT_TYPE}"
  cp -f *.dll *db *.xml *.dtd *.exe *.pl *.properties* "${RESULTS_DIR}" 2>/dev/null
  cd "${RESULTS_DIR}"
  ${MONO} ${NUNIT_CONSOLE} ${NUNIT_FLAGS} ${NUNIT_RUN_FLAG} "${UNITTESTS_DLL}"
  if [ "$?" = "0" ]; then
    RESULT=success
  else
    if [ -n "${OUTDIR}" ]; then
      mkdir -p failures
      testFailures="`getFailures "${RESULT_XML}"`"
      if [ -n "${testFailures}" ]; then
        for failure in ${testFailures}; do
          echo Failure in ${failure}.
          if [ -f "${OUTDIR}/${failure}.txt" ]; then
            cp -f "${OUTDIR}/${failure}.txt" failures
            rm -f "${OUTDIR}/${failure}.txt"
          fi
        done
      fi
    fi
    RESULT=fail
  fi
  cd "${ORIGPWD}"
}

resultPrompt()
{
  RESULT_TYPE="$1"
  RESULTS_DIR="`getResultsDir "${RESULT_TYPE}"`"
  if [ "${RESULT}" = "success" ]; then
    echo
    echo
    echo "SUCCESS: All ${RESULT_TYPE} tests passed."
  else
    echo
    echo
    echo -n "FAILURE: Some ${RESULT_TYPE} tests failed"
    if [ -d "${RESULTS_DIR}/failures" ]; then
      echo "; see ${RESULTS_DIR}/failures"
    else
      echo
    fi
  fi
  if [ -f "${RESULTS_DIR}/TestResult.xml" ]; then
    echo "Test report is in: ${RESULTS_DIR}/TestResult.xml"
  fi
  if [ -n "${GFXDADOOUTDIR}" -a -d "${GFXDADOOUTDIR}" ]; then
    echo "Output is in: ${GFXDADOOUTDIR}"
  fi
}

getArgValue()
{
  echo "$1" | sed 's/^-[^=]*=//'
}

cleanUp()
{
  cd "${INITPWD}"
  if [ "${ISWIN}" = "1" ]; then
    taskkill /FI "USERNAME eq ${USER}" /F /IM ${NUNIT_CONSOLE} >/dev/null 2>/dev/null
  else
    killall -u "${USER}" -9 ${NUNIT_CONSOLE} >/dev/null 2>/dev/null
  fi
  if [ "$1" = "true" ]; then
    sleep 1
    rm -rf "${TESTRES}/csharp"
  fi
}


isFlag="true"
useDebug="false"
UNIT_LOGLEVEL="config"
GFXD_LOGLEVEL="config"
GFXD_SECLOGLEVEL="config"

while [ "${isFlag}" = "true" ]; do
  case $1 in
    -logLevel=*)
      UNIT_LOGLEVEL="`getArgValue "$1"`";
      GFXD_LOGLEVEL="`getArgValue "$1"`";
      shift;
    ;;
    -serverLogLevel=*)
      GFXD_LOGLEVEL="`getArgValue "$1"`";
      shift;
    ;;
    -serverSecLogLevel=*)
      GFXD_SECLOGLEVEL="`getArgValue "$1"`";
      shift;
    ;;
    -debug=*)
      useDebug="`getArgValue "$1"`";
      shift;
    ;;
    -clean)
      echo Cleaning up ...
      cleanUp true;
      exit 0;
    ;;
    *)
      isFlag="false";
    ;;
  esac
done

export GFXDADOOUTDIR="output"
RESULT_TYPE="Release"

if [ -z "${OSBUILDDIR}" ]; then
  echo OSBUILDDIR not set
  exit 1
fi
if [ -z "${GEMFIREXD}" ]; then
  echo GEMFIREXD not set
  exit 1
fi
OSBUILDDIR="`getPath "${OSBUILDDIR}"`"
PATH="${GEMFIREXD}/bin:${PATH}"
TESTRES="${OSBUILDDIR}/tests/results"

if [ "${useDebug}" = "true" ]; then
  RESULT_TYPE="Debug"
else
  RESULT_TYPE="Release"
fi
export UNIT_LOGLEVEL GFXD_LOGLEVEL GFXD_SECLOGLEVEL PATH RESULT_TYPE GFXDADOOUTDIR


mkdir -p "${TESTRES}"
mkdir -p "${TESTRES}/csharp"

INITPWD="`pwd`"

trap "cleanUp false" 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15


# Run the RESULT_TYPE test build

echo Running the ${RESULT_TYPE} tests.
echo
runTests "${RESULT_TYPE}" "$@"
resultPrompt "${RESULT_TYPE}"
if [ "${RESULT}" = "fail" ]; then
  exit 1
fi
exit 0

