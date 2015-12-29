ifndef base
  base = ../..
endif

ifndef VARIANT
  VARIANT=slow
endif

all: postinclude_targets

.PHONY: all postinclude_targets

include $(base)/makefiles/library.gmk

DESTDIR=$(OSBUILDDIR)/tests/classes/sendsigbreak
PRODDIR=$(OSBUILDDIR)/product

INTDIR=$(base)/src/com/gemstone/gemfire/internal
CINCL=-I$(product)/include -I$(INTDIR) -I$(DESTDIR) 

include $(base)/makefiles/executable.gmk

ifeq ($(HOSTTYPE_OSTYPE),sparc.Solaris)
  # do not put platform specific CFLAGS, or LDLIBS here 
  #   they belong in executable.gmk 
else 
ifeq ($(HOSTTYPE_OSTYPE),intel.Mac)
else 
ifeq ($(HOSTTYPE_OSTYPE),intel.Linux)
else 
  #windows 
  LDLIBS += /DEBUG
  CFLAGS += -Zi -Yd
endif
endif
endif


TARGETS=$(DESTDIR)/sendsigbreak${libSize}${EXE} 

postinclude_targets: $(TARGETS)

clean:
	rm -f $(TARGETS) *${OBJ} *.log *.ilk *.pdb core
