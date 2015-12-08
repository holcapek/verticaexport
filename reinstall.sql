\set ON_ERROR_STOP on

DROP LIBRARY IF EXISTS ExportLib CASCADE;

CREATE LIBRARY ExportLib
	AS '/tmp/libexport.so'
	LANGUAGE 'C++';

CREATE FUNCTION export
	AS LANGUAGE 'C++'
	NAME 'ExportFactory'
	LIBRARY Exportlib;

\pset expanded

SELECT *
FROM user_libraries
WHERE lib_name = 'ExportLib';

SELECT *
FROM user_functions
WHERE function_name = 'export';
