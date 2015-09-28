#include <limits.h>
#include <stdio.h>
#include <string.h>
#include "Vertica.h"

using namespace Vertica;
using namespace std;

/*
 * TODO
 * - cache file name in Export
 * - cache column types somehow, and then just dispatch
 *   to particular export function by type (rather than
 *   go throuh if/switch in each iteration of the loop
 * - add gzip and/or bzip compression
 */

class Export : public ScalarFunction
{

private:
FILE *fh;

public:

virtual void
setup (ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
{
	srvInterface.log("Export::setup()");
	ParamReader paramReader(srvInterface.getParamReader());

	if (!paramReader.containsParameter("fpath"))
		vt_report_error(0, "Missing fpath parameter.");

	string fpath = paramReader.getStringRef("fpath").str();
	if (NULL == (this->fh = fopen(fpath.c_str(), "w")))
		vt_report_error(0, "Failed to open file [%s] for writing: %s",
			fpath.c_str(),
			strerror(errno)
		);
}

virtual void
destroy (ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
{
	srvInterface.log("Export::destroy()");
	if (0 != fclose(this->fh))
		vt_report_error(0, "Failed to close file: %s",
			strerror(errno)
		);
}

virtual void
processBlock (ServerInterface &srvInterface, BlockReader &arg_reader, BlockWriter &res_writer)
{
	srvInterface.log("Export::processBlock()");

	do {
		fwrite("this is a foo line\n", 19, 1, this->fh);

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
getPrototype (ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
{
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
