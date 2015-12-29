#
# this makefile is intended to build individual gemfire C programs
# that have one source file.  Use
#               gnumake executablename
#
# on windows, or
#               gnumake HOSTTYPE_OSTYPE=sparc.Solaris executablename
#
# on unix
#
  
ifndef base
  base = ../..
endif

ifndef VARIANT
  VARIANT=slow
endif

all: postinclude_targets

.PHONY: all postinclude_targets

TARGETOBJS= $(foreach i,$(TARGETS),$(patsubst+%$(EXE),%$(OBJ),$(i)))

.PRECIOUS: $(TARGETOBJS)

include $(base)/makefiles/library.gmk


PRODDIR=$(OSBUILDDIR)/product


ifeq ($(HOSTTYPE_OSTYPE),intel.Windows)
    EXE = .exe
    OBJ = .obj
else
    EXE = 
    OBJ = .o
endif


INTDIR=$(base)/src/com/gemstone/gemfire/internal
CINCL=-I$(product)/include -I$(INTDIR) -I$(DESTDIR) -I. -I$(base)/tests/util

ifeq ($(HOSTTYPE_OSTYPE),sparc.Solaris)
  CFLAGS = -g -mt $(CINCL)
  LDFLAGS = $(LFLAGSslow)
  LDLIBS = -L $(PRODDIR)/lib -lgemfire -lpthread -lrt
else
ifeq ($(HOSTTYPE_OSTYPE),intel.Mac)
  CFLAGS = -g $(CFLAGS_MODEL) $(CINCL) -D_REENTRANT 
  LDFLAGS = $(LFLAGSslow) $(CFLAGS_MODEL)

  LDLIBS = -L$(PRODDIR)/lib -lgemfire -lpthread $(LIBS)
else
ifeq ($(HOSTTYPE_OSTYPE),intel.Linux)
  CFLAGS = -g $(CFLAGS_MODEL) $(CINCL) -D_REENTRANT 
  LDFLAGS = $(LFLAGSslow) $(CFLAGS_MODEL)

  LDLIBS = -L $(PRODDIR)/lib -lgemfire -lpthread $(LIBS)
else 
  CINCL := $(foreach dir,$(subst -I,,$(CINCL)),-I$(call NATIVEDIR_FUNC,$(dir)))
  CFLAGS = $(CFLAGS_COMMON) $(CFLAGSslow) $(CINCL)
  LDFLAGS =  $(LFLAGSslow) /LIBPATH:$(call NATIVEDIR_FUNC,$(PRODDIR)/lib)
  LDLIBS = gemfire.lib kernel32.lib
endif
endif
endif

$(DESTDIR)/%${OBJ} : %.c
	@echo compiling 2 $<
	$(CC) $(CFLAGS) $(VCINCLUDES) -c $(OUT.c) $<

$(DESTDIR)/%${OBJ} : $(DESTDIR)/%.c
	@echo compiling 3 $<
	$(CC) $(CFLAGS) $(VCINCLUDES) -c $(OUT.c) $(call NATIVEDIR_FUNC,$<)

$(DESTDIR)/%${EXE} : $(DESTDIR)/%${OBJ}
	@echo linking $@
ifeq ($(HOSTTYPE_OSTYPE),intel.Windows)
	link $(LDFLAGS) $(LIBPATH) -out:$(call NATIVEDIR_FUNC,$@) $(LDLIBS) $(foreach obj,$^,$(call NATIVEDIR_FUNC,$(obj)))

else
ifeq ($(HOSTTYPE_OSTYPE),intel.Linux)
	$(CXX) $(LDFLAGS) -o $@ $^ $(LDLIBS)
else
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)
endif
endif

TARGETS=$(DESTDIR)/HelloWorld${EXE}

postinclude_targets: $(TARGETS)

$(DESTDIR)/HelloWorld${EXE}:  $(DESTDIR)/HelloWorld${OBJ}

clean:
	rm -f $(TARGETS) *${OBJ} *.log *.ilk *.pdb core

