VERTICA_SDK=/opt/vertica/sdk

CXXFLAGS=-I $(VERTICA_SDK)/include -fPIC -Wall
LDFLAGS=-lpthread

all: libexport.so

Vertica.o: $(VERTICA_SDK)/include/Vertica.cpp
	$(CXX) -c $(CXXFLAGS) $?

export.o: export.cpp
	$(CXX) -c $(CXXFLAGS) $?

libexport.so: Vertica.o export.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -shared -o $@ $?

clean:
	rm -f *.o *.so

install: libexport.so
	cp $^ /tmp
	/opt/vertica/bin/vsql -U vertica -f reinstall.sql

test:
