VERTICA_SDK=/opt/vertica/sdk

OPTIMIZE=-O3
CXXFLAGS=-I $(VERTICA_SDK)/include -fPIC -Wall -shared $(OPTIMIZE)
LDFLAGS=-lpthread


all: libunload.so

SOURCE_LIST = $(VERTICA_SDK)/include/Vertica.cpp unload.cpp

libunload.so: $(SOURCE_LIST) Makefile
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(SOURCE_LIST)

clean:
	rm -f *.o *.so

install: libunload.so
	sudo rm -rf pkg
	mkdir -p pkg pkg/lib pkg/ddl
	install libunload.so pkg/lib
	install ddl/*.sql pkg/ddl
	# TODO copy package.conf
	sudo install -o vertica -g vertica -d \
		/opt/vertica/packages/unload \
		/opt/vertica/packages/unload/ddl \
		/opt/vertica/packages/unload/lib
	#sudo install -o vertica -g vertica pkg/package.conf /opt/vertica/packages/unload/
	sudo install -o vertica -g vertica pkg/ddl/* /opt/vertica/packages/unload/ddl/
	sudo install -o vertica -g vertica pkg/lib/* /opt/vertica/packages/unload/lib/
	sudo su - vertica -c '/opt/vertica/bin/vsql -U vertica -f /opt/vertica/packages/unload/ddl/install.sql'
	
