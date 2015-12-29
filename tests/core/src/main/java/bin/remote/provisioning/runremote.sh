#! /bin/bash
# ---------------------------
# runremote.sh -- provision and run a test on a remote configuration.
#
# This script is actually a template.  You should copy this (and the
# other files in this directory) to a new blank directory and modify
# to your taste.
#
# Be sensitive to SVN updates to this directory, as you may wish to copy
# over newer versions from time to time.
#
# This script is merely a cover for rungemfire.pl.  
# ---------------------------

# ---------------------------
# CONFIGURATION SECTION
#
# Modify variables in this section as appropriate for your run.
# ---------------------------

# ---------------------------
# Best practice is to test your configuration if you have _any_ doubts.
# -n says not to actually run the test (like 'make -n' or 'cvs -n').
# ---------------------------
trial=
#trial=-n

# ---------------------------
# Specify the BT file you want to run here:
# ---------------------------
BT=test2.bt
#BT=scaled_useCase17.bt

# ---------------------------
# Specify the configuration file you want to use.
#
# IMPORTANT: a number of the values in the configuration file MUST
# NOT BE CHANGED.  Look at the template for your configuration for
# more details
# ---------------------------
#CONF=bensley.conf
CONF=ptest.conf

# ---------------------------
# Your results will be copied back to your local environment, to a
# directory that you specify.  If the given directory name already exists,
# it will be renamed in a unique fashion.
#
# The parent directory of the result directory must already exist.
#
# (If you are using the queuing tool (runqueue.pl), the parent directory
# must be writable by the uid running the queuer.)
#
# Specify the name of the result directory here.  
# ---------------------------
RESULTDIR=/export/frodo1/users/jpenney/results_ptest

# ---------------------------
# The product you are testing can be optionally copied to the target
# host.  
#
# (Use delremote.pl to remove a product directory you no longer need.)
#
# If you specify the 'copyBUILD' argument to rungemfire.pl, OSBUILD is
# used as the source of the copy, and it will be copied to REMOTE_BUILD
# REMOTE_BUILD will first be removed, if it exists.
#
# If 'copyBUILD' is not specified, this argument is ignored.
#
# OSBUILD is the same as the osbuild.dir property in your ant makefile;
# i.e., it's the host-specific build artifacts directory.  Symbolic links
# in this directory will not work.
# ---------------------------
OSBUILD=/export/frodo1/users/jpenney/gemfire_bensley

#copyBUILD=
copyBUILD=copyBUILD

# ---------------------------
# You specify the product (on the remote machine) that you will be testing.
# If you need to create it, use the OSBUILD argument and make sure that
# the 'copyBUILD' is passed to rungemfire.pl.
#
# When using 'copyBUILD', the parent directory must already exist.
#
# If you are testing the same product multiple times, you can (obviously) omit
# the copy step after the first test run.
#
# Best practice is to choose a name that uniquely identifies
# it, i.e., includes your username somewhere in it.
# ---------------------------
#REMOTE_BUILD=/export/bensa1/users/jpenney/jpenney_gemfire_bensley
REMOTE_BUILD=/export/ptesta1/users/build/jpenney_gemfire_bensley

# ---------------------------
# You can specify the JDK to be used.  Best practice for the Congo
# release is to use JDK 1.5.  There is a 'copyJDK' argument to rungemfire.pl
# to provision a new JDK.
#
# You should not have to change this argument very often.
#
# When using 'copyJDK', the parent directory must already exist.
# ---------------------------
#JDK=/export/bensa1/jdk1.5.0_05
JDK=/export/ptesta1/users/build/jdk1.5.0_05

# ---------------------------
# If you should actually need to create a JDK, this is the JDK on
# your local machine.  It will be copied to the location JDK.
#
# If you specify the 'copyJDK' argument to rungemfire.pl, JDK_SOURCE is
# used as the source of the copy, and it will be copied to JDK.
#
# If 'copyJDK' is not specified, this argument is ignored.
#
# ---------------------------
JDK_SOURCE=/gcm/where/jdk/1.5.0_05/x86.linux

copyJDK=
#copyJDK=copyJDK

# ---------------------------
# This is the list of hosts to use.  This could be modified
# if a host were dead...
# ---------------------------
#hostList="bensa bensc bensd bense bensf bensb"
hostList="ptesta ptestc ptestd pteste ptestf ptestb"

# ---------------------------
# Which host will have the hydra master?
# ---------------------------
#MASTER=bensa
MASTER=ptesta

# ---------------------------
# All of your results are held in $SCRATCHPARENT/$RESULTDIR on the master.
# 
# Results are "collected" here below here, before they are copied back to
# RESULTDIR on your system.
# 
# This directory must already exist.
# 
# If you are running multiple concurrent tests with the same $RESULTDIR name
# (deprecated), you will need to choose a different SCRATCHPARENT for each
# test.
# ---------------------------
#SCRATCHPARENT=/export/bensa1/users/jpenney/perfresults
SCRATCHPARENT=/export/ptesta1/users/build/perfresults

# ---------------------------
# An alternate user (needed in ptest environment) to run as
# ---------------------------
#USER=jpenney
USER=build

# ---------------------------
# SCRATCH -- if you don't set this by hand, the following
# code should be sufficient in many cases.
# ---------------------------
# Compose the list of scratch folders...
SCRATCH=
for each in $hostList
do
  if [ "$SCRATCH" != "" ]; then
    SCRATCH="$SCRATCH "
  fi
    SCRATCH="${SCRATCH}/export/${each}1/users/$USER/perfresults/scratch"
done

# ---------------------------------
# NO USER SERVICEABLE PARTS BELOW
# ---------------------------------

# Run the test
perl rungemfire.pl -v \
  BT="$BT" \
  SCRATCHPARENT="$SCRATCHPARENT" \
  conf="$CONF" \
  JDK="$JDK" \
  OSBUILD="$OSBUILD" \
  RESULTDIR="$RESULTDIR" \
  hostList="$hostList" \
  REMOTE_BUILD="$REMOTE_BUILD" \
  SCRATCH="$SCRATCH" \
  MASTER=$MASTER \
  $copyBUILD \
  JDKSOURCE="$JDK_SOURCE" \
  $copyJDK \
  $trial

# ---------------------------------
# If you are using the queuer to launch your test for you, RESULTDIR will
# be owned by the uid of the process running runqueue.pl.  Thus, we need
# to ensure that the resulting files can be modified (moved, deleted, etc.)
# by you after the test finishes.
# ---------------------------------
chmod -R ugo+w $RESULTDIR
