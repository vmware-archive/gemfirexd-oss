ifndef base
 base = ../../../../..
endif

LIBRARY = gemfire

FILES_cpp = \
  HostStatHelperSolaris.cpp \
  HostStatHelperLinux.cpp \
  HostStatHelperOSX.cpp \
  HostStatHelperWin.cpp \
  NanoTimer.cpp \
  OSProcess.cpp \
  SmHelper.cpp \
  GfVersion.cpp \


FILES_c = \
  dllmain.c \
  hostunix.c \
  utl.c \

# following files listed separately so we can control their position
#  in linker input arguments .
#
FILES_LAST_c = utllastfile.c
FILES_FIRST_c = utlfirstfile.c

# if you want instance field offsets in the .h file, must
#   also add the class to the offsetFieldClasses list in build.xml
JNI_files = \
  com.gemstone.gemfire.internal.HostStatHelper \
  com.gemstone.gemfire.internal.LinuxProcessStats \
  com.gemstone.gemfire.internal.LinuxSystemStats \
  com.gemstone.gemfire.internal.OSXProcessStats \
  com.gemstone.gemfire.internal.OSXSystemStats \
  com.gemstone.gemfire.internal.NanoTimer \
  com.gemstone.gemfire.internal.OSProcess \
  com.gemstone.gemfire.internal.SmHelper \
  com.gemstone.gemfire.internal.SolarisProcessStats \
  com.gemstone.gemfire.internal.SolarisSystemStats \
  com.gemstone.gemfire.internal.WindowsProcessStats \
  com.gemstone.gemfire.internal.WindowsSystemStats \


# if you want instance field offsets in the .h file, must
#   also add the class to the offsetFieldClasses list in build.xml


include $(base)/makefiles/library.gmk

$(OBJDIR)/HostStatHelper.$(OBJEXT): \
  $(GENERATEDDIR)/com_gemstone_gemfire_internal_HostStatHelper.h \
  $(GENERATEDDIR)/com_gemstone_gemfire_internal_SolarisProcessStats.h \
  $(GENERATEDDIR)/com_gemstone_gemfire_internal_SolarisSystemStats.h \
  $(GENERATEDDIR)/com_gemstone_gemfire_internal_WindowsProcessStats.h \
  $(GENERATEDDIR)/com_gemstone_gemfire_internal_WindowsSystemStats.h \
  flag.ht global.ht host.hf utl.hf
$(OBJDIR)/OSProcess.$(OBJEXT): $(GENERATEDDIR)/com_gemstone_gemfire_internal_OSProcess.h flag.ht
$(OBJDIR)/SmHelper.$(OBJEXT): flag.ht global.ht $(HOSTSHR_HF) host.hf utl.hf
$(OBJDIR)/NanoTimer.$(OBJEXT): $(GENERATEDDIR)/com_gemstone_gemfire_internal_NanoTimer.h host.hf flag.ht global.ht

$(OBJDIR)/hostunix.$(OBJEXT): flag.ht global.ht host.ht host.hf utl.hf
$(OBJDIR)/utl.$(OBJEXT): flag.ht global.ht host.hf utl.hf 

