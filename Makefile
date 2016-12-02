VERTICA_SDK=/opt/vertica/sdk

CXXFLAGS=-I $(VERTICA_SDK)/include -fPIC -Wall
LDFLAGS=-lpthread

all: libunload.so

verticaudx.o: $(VERTICA_SDK)/include/Vertica.cpp
	$(CXX) -o $@ -c $(CXXFLAGS) $?

unload.o: unload.cpp
	$(CXX) -c $(CXXFLAGS) $?

libunload.so: verticaudx.o unload.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -shared -o $@ $?

clean:
	rm -f *.o *.so
