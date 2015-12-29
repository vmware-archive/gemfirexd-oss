#! /bin/csh

gccimpl.sh EnvironmentVariables.cpp $*
gccimpl.sh HostStatHelper.cpp $*
gccimpl.sh NanoTimer.cpp $*
gccimpl.sh OSProcess.cpp $*
gccimpl.sh SmHelper.cpp $*

gansicimpl.sh dllmain.c $*
gansicimpl.sh hostunix.c $*
gansicimpl.sh utl.c $*
