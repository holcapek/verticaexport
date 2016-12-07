\set libfile '''/opt/vertica/packages/unload/lib/libunload.so'''
CREATE LIBRARY unload_lib AS :libfile LANGUAGE 'C++';
CREATE TRANSFORM FUNCTION unload AS LANGUAGE 'C++' NAME 'unload_factory' LIBRARY unload_lib FENCED;
