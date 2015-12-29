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

ifndef base
  base = ../..
endif

ifndef VARIANT
  VARIANT=fast
endif

DIRS = 

default:  all
	#$(MAKE) VARIANT=fast test_targets
	#$(MAKE) VARIANT=slow test_targets

all: 
#	$(foreach item,$(DIRS),$(MAKE) -C $(item) $@ &&):

	$(MAKE) VARIANT=fast test_targets
	$(MAKE) VARIANT=slow test_targets

.PHONY: all test_targets 



OBJDIR=$(DESTDIR)
CINCL=-I. -I$(ACE_DIR) -I$(GCC_DIR)/include


# By default NATIVEDIR_FUNC does nothing
NATIVEDIR_FUNC = $(1)
ifeq ($(OSNAME),Linux)
	EXE =
	OBJ = .o
	CXX = g++
	NATIVEINC=linux
	HOSTTYPE_OSTYPE=intel.Linux
	ifeq ($(GFLIB_MODEL), 64bit)
		CFLAGS = -m64 $(CFLAGS_MODEL) $(FPIC) $(CINCL)
		LDFLAGS = -m64 $(LDFLAGSfast) $(CFLAGS_MODEL) -L$(GCC_DIR)/lib64 -L$(GCC_DIR)/lib
	else
		CFLAGS = -m32 $(CFLAGS_MODEL) $(FPIC) $(CINCL) 
		LDFLAGS = -m32 $(LDFLAGSfast) $(CFLAGS_MODEL) -L$(GCC_DIR)/lib
	endif
	
	
	LIBS += $(ACE_DIR)/lib/libACE.a
	OUT.c = -o $@
	DESTDIR= $(ODBC_TEST_OUTDIR)
else 
  ifeq ($(OSNAME),Mac OS X)
        EXE =
        OBJ = .o
        CXX = g++
        NATIVEINC=Darwin
        HOSTTYPE_OSTYPE=Darwin
       
        ifeq ($(GFLIB_MODEL), 64bit)
                CFLAGS = -m64 $(CFLAGS_MODEL) $(FPIC) $(CINCL) 
                LDFLAGS = -m64 $(LDFLAGSfast) $(CFLAGS_MODEL) -L$(GCC_DIR)/lib
        else
                CFLAGS = -m32 $(CFLAGS_MODEL) $(FPIC) $(CINCL) 
                LDFLAGS = -m32 $(LDFLAGSfast) $(CFLAGS_MODEL) -L$(GCC_DIR)/lib/i386 -L$(GCC_DIR)/lib
        endif


        LIBS += $(ACE_DIR)/lib/libACE.a
        OUT.c = -o $@
        DESTDIR= $(ODBC_TEST_OUTDIR)
  else
	EXE = .exe
	OBJ = .obj
	CXX = cl
	HOSTTYPE_OSTYPE=intel.Windows
	NATIVEINC=win32
	NATIVEDIR_FUNC = "$(strip $(shell cygpath -w "$(1)"))"
	CINCL := $(foreach dir,$(subst -I,,$(CINCL)),-I$(call NATIVEDIR_FUNC,$(dir)))
	CFLAGS = $(CFLAGS_COMMON) $(CINCL)
	LDFLAGS = $(LFLAGSfast) 
	OUT.c = -Fo$(call NATIVEDIR_FUNC,$@)
	DESTDIR= $(shell cygpath -u "$(ODBC_TEST_OUTDIR)")
  endif      
endif

# Use CXX (C++ compiler) for all of these compiles to require prototypes 
# for all functions.
# Without prototypes, 64 bit code will not be compiled correctly because
# default function result type of "int" is not the same size as "void*"


$(DESTDIR)/%${OBJ} : %.cpp
	@echo compiling 1 $<
	$(CXX) $(CFLAGS) $(VCINCLUDES) -c $(OUT.c) $<

$(DESTDIR)/%${EXE} : $(DESTDIR)/%${OBJ}
	@echo linking $@
	
ifeq ($(HOSTTYPE_OSTYPE),intel.Windows)
	link $(LDFLAGS) $(LIBPATH) /OUT:$(call NATIVEDIR_FUNC,$@) $(foreach obj,$^,$(call NATIVEDIR_FUNC,$(obj))) $(LDLIBS)
	#$(MT_COMMAND)
else
	$(CXX) $(LDFLAGS) -o $@ $^ $(LDLIBS)
endif

KEEPOBJS= $(DESTDIR)/%${OBJ}

.PRECIOUS: $(KEEPOBJS)

ifeq ($(HOSTTYPE_OSTYPE),intel.Linux)
LDLIBS = -lpthread
else 
  ifeq ($(HOSTTYPE_OSTYPE),Darwin)
    LDLIBS = -lpthread
  else
    CFLAGS_COMMON = -nologo /GR /EHac 
    CFLAGS_COMMON += /DWINVER=0x0500 /D_WIN32_WINNT=0x0500 
    CFLAGS_COMMON += -W3 -D_CRT_SECURE_NO_DEPRECATE -D_CRT_NONSTDC_NO_DEPRECATE 
  endif  
endif

CFLAGS += -D__ACE_INLINE__ -DACE_NLOGGING -DACE_NDEBUG -D__COMPILE_DUNIT_
#EXTRA_OBJS = $(DESTDIR)/fw_dunit${OBJ}
FWK_CPP_FILES := $(sort $(wildcard fw_*.cpp))

EXTRA_OBJS := $(foreach item,$(subst .cpp,${OBJ},$(FWK_CPP_FILES)),$(DESTDIR)/$(item))

ifeq ($(CPPDEVEL),1)
  CFLAGS += -DGF_DEVEL_ASSERTS=1
endif


ifeq ($(HOSTTYPE_OSTYPE),intel.Linux)
  SH_LIB = /gcm/where/cplusplus/smartheap/x86.Linux/lib
  SH_INC = /gcm/where/cplusplus/smartheap/x86.Linux/include
  ifeq ($(USE_SMARTHEAP),true)
    ifeq ($(CPPDEBUG),true)
      SH_SFX = _mtd.a
    else
      SH_SFX = _mt.a
    endif
    CFLAGS += -I$(SH_INC) -DUSE_SMARTHEAP
    SH_LD_ARGS = $(SH_LIB)/libsmartheapC$(SH_SFX) $(SH_LIB)/libsmartheap$(SH_SFX)
  else
    SH_LD_ARGS = 
  endif
  LDLIBS += $(SH_LD_ARGS) -rdynamic -L$(ACE_DIR)/lib -L$(ODBC_OUT_DIR) -lACE  -rdynamic -lpthread -rdynamic -lstdc++ -lgemfirexdodbc -lodbcinst
  CFLAGS += -D_REENTRANT -Werror
  ifeq ($(CPPDEBUG),true)
    CFLAGS += -g -m32
  else
    CFLAGS += -O3
  endif
	#EXTRA_OBJS += UnixSignalHandler${OBJ}
else 
  ifeq ($(HOSTTYPE_OSTYPE),Darwin)
    ifeq ($(USE_SMARTHEAP),true)
      ifeq ($(CPPDEBUG),true)
        SH_SFX = _mtd.a
      else
        SH_SFX = _mt.a
      endif
    else
      SH_LD_ARGS =
    endif
    LDLIBS += $(SH_LD_ARGS) -rdynamic -L$(ACE_DIR)/lib -L$(ODBC_OUT_DIR) -lACE  -rdynamic -lpthread -rdynamic -lstdc++ -lgemfirexdodbc -lodbcinst
    CFLAGS += -D_REENTRANT 
    ifeq ($(CPPDEBUG),true)
      CFLAGS += -g -m32
    else
      CFLAGS += -O3
    endif
    #EXTRA_OBJS += UnixSignalHandler${OBJ}
  else
    LDLIBS = /DEBUG /LIBPATH:$(call NATIVEDIR_FUNC,$(ACE_DIR)/lib) /LIBPATH:$(call NATIVEDIR_FUNC,$(ODBC_OUT_DIR)) $(ACELINKNAME).lib gemfirexdodbc.lib /MAP
    ifeq ($(CPPDEBUG),true)
      CFLAGS += $(CFLAGSslow)
    else
      CFLAGS += $(CFLAGSfast)
    endif
    EXTRA_OBJS += $(DESTDIR)/WindowsSignalHandler${OBJ}
  endif  
endif



TESTS_CPP_FILES := $(sort $(wildcard test*.cpp))

TARGETS := $(foreach item,$(subst .cpp,${EXE},$(TESTS_CPP_FILES)),$(DESTDIR)/$(item))

test_targets: $(TARGETS) 
$(TARGETS): $(EXTRA_OBJS)

clean:
	rm -f $(TARGETS) *${OBJ} *.log 

  
  
