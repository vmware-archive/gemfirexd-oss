USESUN=true

all:	default

# Set this to relative path to top of tree
BASE=../..
RELATIVE=query/lang

TARGETS = classes 

PHONY = $(TARGETS)
.PHONY: $(PHONY)

default:	$(TARGETS)

all_classes = \
  $(BASE_PATH)/classes/$(RELATIVE)/Address.class \
  $(BASE_PATH)/classes/$(RELATIVE)/Bootstrap.class \
  $(BASE_PATH)/classes/$(RELATIVE)/Employee.class \
  $(BASE_PATH)/classes/$(RELATIVE)/DerivedEmployee.class \
  $(BASE_PATH)/classes/$(RELATIVE)/Person.class \
  $(BASE_PATH)/classes/$(RELATIVE)/Student.class \
  $(BASE_PATH)/classes/$(RELATIVE)/G_Student.class \
  $(BASE_PATH)/classes/$(RELATIVE)/UG_Student.class \
  $(BASE_PATH)/classes/$(RELATIVE)/Department.class \
  $(BASE_PATH)/classes/$(RELATIVE)/Faculty.class \
  $(BASE_PATH)/classes/$(RELATIVE)/Course.class \
  $(BASE_PATH)/classes/$(RELATIVE)/TestQuery.class \
  $(BASE_PATH)/classes/$(RELATIVE)/TestQuery2.class \
  $(BASE_PATH)/classes/$(RELATIVE)/Utils.class


include $(BASE)/makefiles/Makefile.common

EXTRA_CLASSPATH = $(BASE)/../gsjlayers/query/classes
