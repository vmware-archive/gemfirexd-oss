GCCBASE = $(GCMDIR)/where/cplusplus/gcc
PROGRESSBASE = $(GCMDIR)/where/cplusplus
SRCS:=$(wildcard *.cpp) gemfirexdodbcSetup/ConfigSettings.cpp
OBJS:=$(SRCS:.cpp=$(OBJDIR).o)
INCLUDES:=-I. -I$(ADD_INCLUDES)/Auto*.h -I$(ADD_INCLUDES)/ConfigSettings.h
OUTBASE =$(OUTBASE_EX)
OUT_ARTIFACT=odbc-installer
PROGRESS_OUT_ARTIFACT=progress-installer
UNAME := $(shell uname)

ifeq ($(OSNAME),Linux)
	ifeq ($(GFLIB_MODEL),64bit)
		CXXEXTRA_FLAGS = -m64
		GCCDIR = $(GCCBASE)/linux64/4.5.3
		GCCLIBDIR_W = $(GCCBASE)/linux64/4.5.3/lib64
		GCCLIBDIR = $(GCCLIBDIR_W)
		GCCLIBDIR2 = $(GCCBASE)/linux64/4.5.3/lib
		CFLAGS_common += -I$(GCCBASE)/linux64/4.5.3/include
		PROGRESSDIR = $(PROGRESSBASE)/odbc_framework/linux/64/progress/dm
		PROGRESSLIBDIR_W = $(PROGRESSBASE)/odbc_framework/linux/64/progress/dm/lib64
		PROGRESSLIBDIR = $(PROGRESSLIBDIR_W)
		PROGRESSLIBDIR2 = $(PROGRESSBASE)/odbc_framework/linux/64/progress/dm/lib
		PROGRESSFLAGS_common += -I$(PROGRESSBASE)/odbc_framework/linux/64/progress/dm/include
		OUTDIR = $(OUTBASE)/64
		CFLAGS_link_libs = -L$(GCCLIBDIR2) -lodbcinst
		ICFLAGS_link_libs = -L$(GCCLIBDIR2) -liodbcinst
		PROGRESS_link_libs = -L$(PROGRESSLIBDIR2) -lodbcinst
	else
		CXXEXTRA_FLAGS = -m32
		GCCDIR = $(GCCBASE)/linux/4.5.3
		GCCLIBDIR_W = $(GCCBASE)/linux/4.5.3/lib
		GCCLIBDIR = $(GCCLIBDIR_W)
		CFLAGS_common += -I$(GCCBASE)/linux/4.5.3/include
		PROGRESSDIR = $(PROGRESSBASE)/odbc_framework/linux/32/progress/dm
		PROGRESSLIBDIR_W = $(PROGRESSBASE)/odbc_framework/linux/32/progress/dm/lib
		PROGRESSLIBDIR = $(PROGRESSLIBDIR_W)
		PROGRESSLIBDIR2 = $(PROGRESSBASE)/odbc_framework/linux/32/progress/dm/lib
		PROGRESSFLAGS_common += -I$(PROGRESSBASE)/odbc_framework/linux/32/progress/dm/include
		OUTDIR = $(OUTBASE)/32
	    CFLAGS_link_libs = -L$(GCCLIBDIR) -lodbcinst
		ICFLAGS_link_libs = -L$(GCCLIBDIR) -liodbcinst
		PROGRESS_link_libs = -L$(PROGRESSLIBDIR) -lodbcinst
    endif
endif
ifeq ($(UNAME),Darwin)
	ifeq ($(GFLIB_MODEL),64bit)
		CXXEXTRA_FLAGS = -m64
		GCCDIR = $(GCCBASE)/macosx/4.8.1
		GCCLIBDIR_W = $(GCCBASE)/macosx/4.8.1/lib
		GCCLIBDIR = $(GCCLIBDIR_W)
		GCCLIBDIR2 = $(GCCBASE)/macosx/4.8.1/lib
		CFLAGS_common += -I$(GCCBASE)/macosx/4.8.1/include
		OUTDIR = $(OUTBASE)/64
		CFLAGS_link_libs = -L$(GCCLIBDIR2) -lodbcinst
		ICFLAGS_link_libs = -L$(GCCLIBDIR2) -liodbcinst
	else
		CXXEXTRA_FLAGS = -m32
		GCCDIR = $(GCCBASE)/macosx/4.8.1
		GCCLIBDIR_W = $(GCCBASE)/macosx/4.8.1/lib/i386
		GCCLIBDIR = $(GCCLIBDIR_W)
		GCCLIBDIR2 = $(GCCBASE)/macosx/4.8.1/lib
		CFLAGS_common += -I$(GCCBASE)/macosx/4.8.1/include
		OUTDIR = $(OUTBASE)/32
	  CFLAGS_link_libs = -L$(GCCLIBDIR) -lodbcinst
		ICFLAGS_link_libs = -L$(GCCLIBDIR) -L$(GCCLIBDIR2) -liodbcinst
    endif
endif
all: $(SRCS)
	echo "Building gemfirexd odbc installer application..."
	$(CXX) $(CXXEXTRA_FLAGS) $(CFLAGS_common) $(INCLUDES) $^ -o $(OUTDIR)/$(OUT_ARTIFACT) $(CFLAGS_link_libs)
	$(CXX) $(CXXEXTRA_FLAGS) $(CFLAGS_common) $(INCLUDES) $^ -o $(OUTDIR)/iodbc/i$(OUT_ARTIFACT) $(ICFLAGS_link_libs) -DIODBC=1
ifeq ($(OSNAME),Linux)
	export LD_LIBRARY_PATH="$(PROGRESSLIBDIR2):$${LD_LIBRARY_PATH}" && \
	$(CXX) $(CXXEXTRA_FLAGS) $(PROGRESSFLAGS_common) $(INCLUDES) $^ -o $(OUTDIR)/progress/$(PROGRESS_OUT_ARTIFACT) $(PROGRESS_link_libs) -DPDSODBC=1
endif
	rm -f *.[o] *.log core
