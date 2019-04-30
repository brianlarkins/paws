#
# makefile overlord for portals scioto
# author: d. brian larkins
# created: 4/24/18
#

CFLAGS=$(GCFLAGS) -I. -I../include -I$(PORTALS_INCLUDEDIR)

#LDFLAGS=-lportals -lpmi

RANLIB = /usr/bin/ranlib

HDRS =  clod.h        \
				mutex.h       \
				shr_ring.h    \
				sdc_shr_ring.h\
				sn_ring.h     \
				tc.h          \
				termination.h \
				# line eater


OBJS = btermination.o       \
			 clod.o               \
			 collection-sdc.o     \
			 collection-steal-n.o \
			 common.o             \
       commsynch.o          \
			 handle.o             \
			 init.o               \
			 matchpairs.o         \
			 mutex.o              \
			 pmi.o                \
			 sdc_shr_ring.o       \
			 shr_ring.o           \
			 sh_ring.o            \
			 sn_common.o          \
			 sn_ring.o            \
			 task.o               \
			 tc-clod.o            \
			 termination.o        \
       util.o               \
       # line eater

.PHONY: all
all: libtc.a

libtc.a: headers $(OBJS)
	mkdir -p ../lib
	$(AR) rcs ../lib/libtc.a $(OBJS)
	$(RANLIB) ../lib/libtc.a

$(OBJS): $(HDRS)

.PHONY: headers
headers: 
	mkdir -p ../include
	cp $(HDRS) ../include/.

.PHONY: clean
clean:
	rm -f *~ *.o ../include/tc.h ../lib/libtc.a
