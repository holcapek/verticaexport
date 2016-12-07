// vim:ts=4

// PATH_MAX
#include <linux/limits.h>

// pid_t
#include <sys/types.h>

// getpid
#include <unistd.h>

// clas exception
#include <exception>

#include "Vertica.h"

#define LOG(si,args...) \
	do { \
		(si).log(args) \
	} while (0);

#define LOGFN(si,args...) \
	do { \
		pid_t pid = getpid(); \
		pid_t tid = syscall(NR_gettid); \
		LOG(si,"p=%5u t=%5u f=%s", pid, tid, __FUNCTION__) \
	} while (0);

using namespace Vertica;
using namespace std;

class unload : public TransformFunction {

	virtual void
	setup (
		ServerInterface        &srvInterface,
		const SizedColumnTypes &argTypes
	) {
	}

	virtual void
	destroy (
		ServerInterface &srvInterface,
		const SizedColumnTypes &argTypes
	) {
	}

	virtual void
	processPartition (
		ServerInterface &srvInterface,
		PartitionReader &input_reader,
		PartitionWriter &output_writer
	) {
		try {
			do {
			} while (input_reader.next());
        } catch(exception& e) {
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
	}

};

class unload_factory : public TransformFunctionFactory {

	// function argument: column/values it operates upon
	// function parameter: key/value pairs after USING PARAMETERS

	virtual TransformFunction* 
	createTransformFunction (
		ServerInterface &srvInterface
	) {
		return vt_createFuncObj(srvInterface.allocator, unload);
	}

	virtual void
	getPrototype (
		ServerInterface &srvInterface,
		ColumnTypes     &argTypes,
		ColumnTypes     &returnType
	) {
		// accepts variables number of arguments
		argTypes.addAny();

		// returns varchar/int pairs
		returnType.addVarchar();
		returnType.addInt();
	}

	virtual void
	getReturnType (
		ServerInterface        &srvInterface,
		const SizedColumnTypes &argTypes,
		SizedColumnTypes       &returnType
	) {
		// names and sizes of output column values
		returnType.addVarchar(32, "unload_key");
		returnType.addInt("unload_value");
	}

	virtual void
	getParameterType (
		ServerInterface  &srvInterface,
		SizedColumnTypes &parameterTypes
	) {
		parameterTypes.addVarchar(PATH_MAX, "outfile");
		// gz, bz2
		parameterTypes.addVarchar(32, "compress");
	}
};

RegisterFactory(unload_factory);
