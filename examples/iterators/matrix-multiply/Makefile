#CXX          = mpiCC
CXX          = mpicxx

SCIOTO_SOURCE  ?= ../../../src
SCIOTO_INCLUDE ?= ../../../include
SCIOTO_LIBS    ?= ../../../lib
SCIOTO_LDFLAGS ?= -ltc -lm

CXXFLAGS     = -g -DMPICH_IGNORE_CXX_SEEK -I$(GA_INCLUDE) -I$(SCIOTO_INCLUDE) -I$(SCIOTO_SOURCE)

#FORT_LIBS    = -lpathfstart -lpathfortran -lpathfstart -lmv -lmpath -lm -lmv -lmpath -lm -lpscrt -lmv -lmpath
LDFLAGS      = -L$(SCIOTO_LIBS) -L$(GA_LIBS) $(SCIOTO_LDFLAGS) $(GA_LDFLAGS) $(FORT_LIBS)

TARGETS   = matmul-partitioned.x matmul-glob-getnext.x matmul-frag-getnext-absolute.x \
            matmul-frag-getnext-relative.x matmul-scioto.x matmul-scioto-progressive.x

all: $(TARGETS)

matmul-partitioned.x: MatMulDriver.cc LoadBalancers.cc MatMulIterator.h
	$(CXX) -o $@ MatMulDriver.cc LoadBalancers.cc $(CXXFLAGS) -DLDBAL_PARTITIONED $(LDFLAGS)

matmul-glob-getnext.x: MatMulDriver.cc LoadBalancers.cc MatMulIterator.h
	$(CXX) -o $@ MatMulDriver.cc LoadBalancers.cc $(CXXFLAGS) -DLDBAL_GLOB_GETNEXT $(LDFLAGS)

matmul-frag-getnext-absolute.x: MatMulDriver.cc LoadBalancers.cc MatMulIterator.h
	$(CXX) -o $@ MatMulDriver.cc LoadBalancers.cc $(CXXFLAGS) -DLDBAL_FRAG_GETNEXT $(LDFLAGS)

matmul-frag-getnext-relative.x: MatMulDriver.cc LoadBalancers.cc MatMulIterator.h
	$(CXX) -o $@ MatMulDriver.cc LoadBalancers.cc $(CXXFLAGS) -DLDBAL_FRAG_GETNEXT -DRELATIVE $(LDFLAGS)

matmul-scioto.x: MatMulDriver.cc SciotoLoadBalancer.cc MatMulIterator.h
	$(CXX) -o $@ MatMulDriver.cc SciotoLoadBalancer.cc $(CXXFLAGS) -DLDBAL_SCIOTO $(LDFLAGS)

matmul-scioto-progressive.x: MatMulDriver.cc SciotoLoadBalancer.cc MatMulIterator.h
	$(CXX) -o $@ MatMulDriver.cc SciotoLoadBalancer.cc $(CXXFLAGS) -DLDBAL_SCIOTO_PROGRESSIVE $(LDFLAGS)

clean:
	rm -f *.o $(TARGETS)
