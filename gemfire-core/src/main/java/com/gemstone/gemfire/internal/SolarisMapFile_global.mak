# Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See accompanying
# LICENSE file.
#
# This file is used on BOTH Solaris and Linux
#
# Define symbols that are exported from libgemfire.so 
#  only the symbols listed in the global: section are accessible
#  to other shared libraries.  
#
# This file is concatenated with a generated file containing
# all JNI native method functions  and  the file
#  SolarisMapFile_local.mak to produce SolarisMapFile.mak
# 
#  The local: section in SolarisMapFile_local.mak makes any symbol
#  not listed in the global section a private symbol.
#  The local: section does not affect dbx or pstack use of private symbols
#
GemFire_1.0 {
  global:
     # begin host.hf
     HostMilliSleep;
     HostNanoSleep;
     HostSleep;
     HostCurrentTimeNanos;
     HostCurrentTimeMs;
     HostWaitForDebugger;
     HostCallDebugger;
     HostCallDebuggerMsg;
     HostGetCurrentThreadId;
     HostGetEnv;
     HostGetLinuxPid;
     HostYield;
     HostGetCpuCount;
     HostCreateThread;
     HostJoinThread;
     HostGetTimeStr;
     # end host.hf

# local section is contained in 
#   SolarisMapFile_local.mak
#  } 
