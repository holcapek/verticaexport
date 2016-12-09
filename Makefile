VERTICA_SDK=/opt/vertica/sdk

OPTIMIZE=-O3
CXXFLAGS=-I $(VERTICA_SDK)/include -fPIC -Wall -shared $(OPTIMIZE)
LDFLAGS=-lpthread -lcsv

VSQL=/opt/vertica/bin/vsql
RUNVSQL=$(VSQL) -U vertica


all: libunload.so

SOURCE_LIST = $(VERTICA_SDK)/include/Vertica.cpp unload.cpp unload.h

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
	sudo su - vertica -c '/opt/vertica/bin/admintools -t uninstall_package -d verticadb -P unload'
	sudo su - vertica -c '/opt/vertica/bin/admintools -t install_package   -d verticadb -P unload'

vsql:
	$(RUNVSQL)

test:
	$(RUNVSQL) -c "select unload(day_of_spot, station) over (partition nodes) from (select primary_key, day_of_spot,station from tv_spots limit 1000) foo"
