\set libfile '''/opt/vertica/packages/unloader/lib/libverticaunload.so'''
CREATE OR REPLACE LIBRARY unload_lib AS :libfile;
CREATE OR REPLACE FUNCTION unload AS 'C++' NAME '???' LIBRARY unload_lib FENCED;
