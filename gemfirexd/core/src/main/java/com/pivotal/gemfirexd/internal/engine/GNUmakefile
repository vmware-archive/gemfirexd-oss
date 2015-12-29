ifndef base
 base = ../../../../..
endif

LIBRARY = gemfirexd

FILES_cpp = \


FILES_c = \
  utils.c \

# following files listed separately so we can control their position
#  in linker input arguments .
#
FILES_LAST_c = 
FILES_FIRST_c = 

# if you want instance field offsets in the .h file, must
#   also add the class to the offsetFieldClasses list in build.xml
JNI_files = \
  com.gemstone.gemfire.internal.NanoTimer \
  com.pivotal.gemfirexd.internal.GemFireXDVersion \

include $(base)/makefiles/library.gmk

ifeq ($(HOSTTYPE_OSTYPE),sparc.Solaris)
  COpt_fast/utils.c = $(COpt_fast/big_pic)
  # end Solaris
else
ifeq ($(HOSTTYPE_OSTYPE),intel.Mac)
  COpt_fast/utils.c = $(COpt_fast/slower) -fno-strict-aliasing -pthread -D_REENTRANT
  # end Mac
else
ifeq ($(HOSTTYPE_OSTYPE),intel.Linux)
  COpt_fast/utils.c = $(COpt_fast/slower) -fno-strict-aliasing -pthread -D_REENTRANT
  LDFLAGS$(VARIANT) += -z defs -Xlinker -soname=$(LIBNAME) -static-libgcc -mimpure-text
  # end Linux
else
  #end Win2K
endif
endif
endif

$(OBJDIR)/utils.$(OBJEXT): $(GENERATEDDIR)/com_gemstone_gemfire_internal_NanoTimer.h $(GENERATEDDIR)/com_pivotal_gemfirexd_internal_GemFireXDVersion.h 
