\set libfile '''/opt/vertica/packages/unload/lib/libunload.so'''
CREATE LIBRARY unload_lib AS :libfile LANGUAGE 'C++';
CREATE OR REPLACE FUNCTION unload AS LANGUAGE 'C++' NAME 'unload' LIBRARY unload_lib FENCED;
