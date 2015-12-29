ifndef base
 base = ../..
endif

LIBRARY = gemfiretest
TESTBUILD = true

FILES_cpp = \

FILES_c = \
  WindowsPlatform.c

JNI_files = \
  hydra.WindowsPlatform

include $(base)/makefiles/library.gmk

$(OBJDIR)/WindowsPlatform.$(OBJEXT): $(GENERATEDDIR)/hydra_WindowsPlatform.h

