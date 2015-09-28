\set ON_ERROR_STOP on

CREATE OR REPLACE LIBRARY ExportLib
	AS '/tmp/libexport.so'
	LANGUAGE 'C++';

CREATE OR REPLACE FUNCTION export
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
