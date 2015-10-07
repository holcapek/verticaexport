#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <fstream>
#include <limits>

#include "Vertica.h"

// as per https://my.vertica.com/docs/7.1.x/HTML/index.htm#Authoring/ExtendingHPVertica/UDx/CreatingAPolymorphicUDF.htm
// number of arguments of the function, ie. number columns
#define MAX_ARGS              1600
#define NUMERIC_CHAR_BUF_SIZE 127
#define DATE_CHAR_BUF_SIZE    16

using namespace Vertica;
using namespace std;

enum ExportType {
	INT,
	NUMERIC,
	FLOAT,
	VARCHAR,
	DATE
};

class Export;
typedef void (Export::*col_writer_func_t)(BlockReader&, size_t);

class Export : public ScalarFunction
{

private:
fstream           fh;
// number of columns, must be <= MAX_COLS
size_t            num_cols;
ExportType        col_type[MAX_ARGS];
col_writer_func_t col_writer[MAX_ARGS];
char              tmp_numeric_cstr[NUMERIC_CHAR_BUF_SIZE+1]; // extra char for \0
char              tmp_date_cstr[DATE_CHAR_BUF_SIZE];
struct tm         tmp_tm;
time_t            tmp_epoch;

void write_int(BlockReader &br, size_t i)
{
	this->fh << br.getIntRef(i);
}

void write_numeric(BlockReader &br, size_t i)
{
	br.getNumericRef(i).toString(this->tmp_numeric_cstr, NUMERIC_CHAR_BUF_SIZE);
	this->fh << this->tmp_numeric_cstr;
}

void write_float(BlockReader &br, size_t i)
{
	this->fh << br.getFloatRef(i);
}

void write_varchar(BlockReader &br, size_t i)
{
	this->fh << br.getStringRef(i).str();
}

void write_date(BlockReader &br, size_t i)
{
	// $(VERTICA_SDK)/include/TimestampUDxShared.h
	// DateADT represent number of days since 2000-01-01
	// including backward dates

	//     86400 = # of seconds per day
	// 946684800 = unix epoch of 2000-01-01

	// get epoch of the date
	this->tmp_epoch = (time_t)br.getDateRef(i)*86400+946684800;

	// extract date fragments, format it, print it out
	localtime_r(&this->tmp_epoch, &this->tmp_tm);
	strftime(this->tmp_date_cstr, DATE_CHAR_BUF_SIZE, "%Y-%m-%d", &this->tmp_tm);
	this->fh << this->tmp_date_cstr;
}

public:

virtual void
setup (
	ServerInterface &srvInterface,
	const SizedColumnTypes &argTypes
) {
	ParamReader paramReader(srvInterface.getParamReader());
	if (!paramReader.containsParameter("fpath"))
		vt_report_error(0, "Missing fpath parameter.");

	// setup dispatcher for particular type
	this->num_cols = argTypes.getColumnCount();
	for (size_t i = 0; i < this->num_cols; i++)
	{
		const VerticaType col_type = argTypes.getColumnType(i);
		if (col_type.isInt())
		{
			this->col_type[i]   = INT;
			this->col_writer[i] = &Export::write_int;
		}
		else if (col_type.isNumeric())
		{
			this->col_type[i]   = NUMERIC;
			this->col_writer[i] = &Export::write_numeric;
		}
		else if (col_type.isFloat())
		{
			this->col_type[i]   = FLOAT;
			this->col_writer[i] = &Export::write_float;
		}
		else if (col_type.isVarchar())
		{
			this->col_type[i]   = VARCHAR;
			this->col_writer[i] = &Export::write_varchar;
		}
		else if (col_type.isDate())
		{
			this->col_type[i]   = DATE;
			this->col_writer[i] = &Export::write_date;
		}
		else
			vt_report_error(0, "Unsupported export type.");
	}

	string fpath = paramReader.getStringRef("fpath").str();
	try
	{
		this->fh.open(fpath.c_str(), fstream::out | fstream::trunc);
		this->fh.precision(numeric_limits<vfloat>::digits10);
	}
	catch (exception e)
	{
		vt_report_error(0, "Failed to open file [%s] for writing: %s",
			fpath.c_str(),
			e.what()
		);
	}

	// just to be sure \0 is there
	this->tmp_numeric_cstr[NUMERIC_CHAR_BUF_SIZE] = '\0';
}

virtual void
destroy (ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
{
	this->fh.close();
}

virtual void
processBlock (ServerInterface &srvInterface, BlockReader &arg_reader, BlockWriter &res_writer)
{
	do {
		size_t i;
		for (i = 0; i < this->num_cols-1; i++)
		{
			if (!arg_reader.isNull(i))
				(this->*col_writer[i])(arg_reader, i);
			this->fh << ',';
		}
		if (!arg_reader.isNull(i))
			(this->*col_writer[i])(arg_reader, i);
		this->fh << endl;

		res_writer.setInt(1);
		res_writer.next();
	} while (arg_reader.next());
}

}; // class Export

class ExportFactory : public ScalarFunctionFactory
{
public:

virtual ScalarFunction*
createScalarFunction (ServerInterface &srvInterface)
{
	return vt_createFuncObj(srvInterface.allocator, Export);
}

virtual void
getPrototype (
	ServerInterface &srvInterface,
	ColumnTypes     &argTypes,
	ColumnTypes &returnType
) {
	argTypes.addAny();
	returnType.addInt();
}

virtual void
getParameterType (
	ServerInterface &srvInterface,
	SizedColumnTypes &parameterTypes
) {
	parameterTypes.addVarchar(NAME_MAX, "fpath");
}

}; // class ExportFactory

RegisterFactory(ExportFactory);
