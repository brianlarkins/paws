# 
# makefile for task collection examples
# 
#
TC_TOP=..

include $(TC_TOP)/tc.mk

SUBMAKES    = iterators/uts             \
              madness                   \
              #end

.PHONY: all clean
all: 
	for d in $(SUBMAKES); do $(MAKE) -C $$d; done

clean: tcclean
	for d in $(SUBMAKES); do $(MAKE) -C $$d clean; done

