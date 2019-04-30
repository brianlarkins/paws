# 
# makefile for task collection tests
# 
#
TC_TOP=../..

include $(TC_TOP)/tc.mk

TARGETS     = test-clod test-gtc-clod


############################################################
# CLO Database Tests
############################################################

.PHONY: all
all: $(TARGETS)

test-clod: tclibs test-clod.o
	$(CC) $(CFLAGS) -o $@ test-clod.o $(TC_LIBS)

test-gtc-clod: tclibs test-gtc-clod.o
	$(CC) $(CFLAGS) -o $@ test-gtc-clod.o $(TC_LIBS)

clean: tcclean
	rm -f *~ *.o gmon.out $(TARGETS)
