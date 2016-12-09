// vim:ts=4

/*
 * Vertica only supports x86_64
 * size_t = unsigned long long = 8 bytes
 * - column count
 */

// PATH_MAX
#include <linux/limits.h>

// pid_t
#include <sys/types.h>

// getpid
#include <unistd.h>

// SYS_gettid
#include <sys/syscall.h>

// clas exception
#include <exception>

// O_CREAT, O_TRUNC
#include <fcntl.h>

// csv_write2
#include <csv.h>

#include "Vertica.h"
#include "unload.h"

using namespace Vertica;
using namespace std;

#define MAX_COLS 100
#define MAX_CHAR_LEN 255 // in chars, not bytes???
#define QUOTE_CHAR '"' // must be char (one byte)
#define FIELD_DELIMITER_CHAR ',' // dtto
#define RECORD_DELIMITER_CHAR '\n' // dtto
// 2 for two quote char
// 1 for field or record delimiter
// 4 for maximum bytes per utf8 character
//#define BUFFER_SIZE ((MAX_CHAR_LEN+2+1)*4*MAX_COLS*60)
//#define BUFFER_SIZE (1024*1024)
#define BUFFER_SIZE 1024

MUTEX(setup_m);
int setup_once = 0;

MUTEX(destroy_m);
int destroy_once = 0;

MUTEX(file_m);
int fd;

class unload;
typedef void (unload::*serializer_t)(ServerInterface&, PartitionReader&, size_t);

class unload : public TransformFunction {

private:

	serializer_t serializer[MAX_COLS];
	size_t num_column;

	char* buffer;
	long int current_offset;     // last byte of the record
	long int last_record_offset; // last byte of previous record

	inline void
	serialize_null(
		ServerInterface& si
	) {
		if (current_offset + 1 <= BUFFER_SIZE) {
			buffer[current_offset] = FIELD_DELIMITER_CHAR;
			current_offset += 1;
		} else {
			write_to_file(si, buffer, last_record_offset+1);
			reset_last_record_offset(si);

			buffer[current_offset] = FIELD_DELIMITER_CHAR;
			current_offset += 1;
		}
	}

	void
	serialize_int(
		ServerInterface& si,
		PartitionReader& pr,
		size_t ci
	) {
		if (pr.isNull(ci)) {
			serialize_null(si);
			return;
		}

		// MAX_VINT = 9223372036854775807
		// 1 is for sign,
		// 19 for max number of digits,
		// 1 for comma
		// 1 for null byte by sprintf
		if (current_offset + 1 + 19 + 1 + 1 <= BUFFER_SIZE) {
			int len = sprintf(buffer+current_offset, "%lli%c", *pr.getIntPtr(ci), FIELD_DELIMITER_CHAR);
			current_offset += len;
		} else {
			write_to_file(si, buffer, last_record_offset + 1);
			reset_last_record_offset(si);

			int len = sprintf(buffer+current_offset, "%lli%c", *pr.getIntPtr(ci), FIELD_DELIMITER_CHAR);
			current_offset += len;
		}
	}


	void
	serialize_varchar(
		ServerInterface& si,
		PartitionReader& pr,
		size_t ci
	) {
		
		if (pr.isNull(ci)) {
			serialize_null(si);
			return;
		}

		VString s = pr.getStringRef(ci);
		char* data = s.data();
		unsigned int len = s.length();

		// 1 is for field separator
		size_t space_avail = BUFFER_SIZE-current_offset-1;

		size_t len_wrote = csv_write2(
			buffer+current_offset,
			space_avail,
			data,
			len,
			QUOTE_CHAR
		);

		if (len_wrote > space_avail) {
			// TODO what if len_wrote > BUFFER_SIZE

			write_to_file(si, buffer, last_record_offset+1);
			reset_last_record_offset(si);

			len_wrote = csv_write2(
				buffer+current_offset,
				space_avail,
				data,
				len,
				QUOTE_CHAR
			);
		}
		current_offset += len_wrote;
		buffer[current_offset] = FIELD_DELIMITER_CHAR;
		current_offset += 1;
	}

	inline void
	reset_last_record_offset(ServerInterface &si) {
			memcpy(buffer, buffer+last_record_offset+1, current_offset - last_record_offset);
			current_offset = current_offset - last_record_offset;
			last_record_offset = -1;
	}

	inline void
	write_to_file(ServerInterface& si, void *buffer, size_t len) {
		LOCKM(file_m);
		LOG(si, "wrote %li bytes", len);
		write(fd, buffer, len);
		fdatasync(fd);
		UNLOCKM(file_m);
	}

public:

	unload() :
		num_column(0),
		current_offset(0),
		last_record_offset(-1)
	{}

	virtual void
	setup (
		ServerInterface        &srvInterface,
		const SizedColumnTypes &argTypes
	) {
		LOG(srvInterface, "");

		if (argTypes.getColumnCount() > MAX_COLS) {
            vt_report_error(0, "Too many column values: %i, maximum supported: %i", argTypes.getColumnCount(), MAX_COLS);
		}

		buffer = (char *)malloc(BUFFER_SIZE);
		if (NULL == (void *)buffer) {
            vt_report_error(0, "Failed to allocate %i bytes for serialization buffer", BUFFER_SIZE);
		}

		// initialize serializer array
		for ( size_t i = 0; i < argTypes.getColumnCount(); i++ ) {
			VerticaType t = argTypes.getColumnType(i);
			LOG(srvInterface, "got column %i type %s", (int)i, t.getPrettyPrintStr().c_str());
			if (t.isInt()) {
				serializer[i] = &unload::serialize_int;
				num_column += 1;
			} else if (t.isVarchar()) {
				serializer[i] = &unload::serialize_varchar;
				num_column += 1;
			} else {
            	vt_report_error(0, "Unsupported type: %s", t.getPrettyPrintStr().c_str());
			}
		}

		LOCKM(setup_m);
		if (!setup_once) {
			fd = open("/tmp/dump.csv", O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH );
			LOG(srvInterface, "setup body was ran just once");
			setup_once = 1;
		}
		UNLOCKM(setup_m);

	}

	virtual void
	destroy (
		ServerInterface &srvInterface,
		const SizedColumnTypes &argTypes
	) {
		LOG(srvInterface, "");

		write_to_file(srvInterface, buffer, last_record_offset + 1);

		LOCKM(destroy_m);
		if (!destroy_once) {
			if (NULL != buffer) {
				free(buffer);
			}
			close(fd);
			LOG(srvInterface, "destroy body was ran just once");
			destroy_once = 1;
		}
		UNLOCKM(destroy_m);

	}

	void
	mark_end_of_record() {
		buffer[current_offset-1] = RECORD_DELIMITER_CHAR;
		last_record_offset = current_offset - 1;
	}

	virtual void
	processPartition (
		ServerInterface &srvInterface,
		PartitionReader &input_reader,
		PartitionWriter &output_writer
	) {
		LOG(srvInterface, "");

		try {

			do {
				for (size_t i = 0; i < num_column; i++) {
					(this->*serializer[i])(srvInterface, input_reader, i);
				}

				mark_end_of_record();

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
		LOG(srvInterface, "");

		return vt_createFuncObj(srvInterface.allocator, unload);
	}

	virtual void
	getPrototype (
		ServerInterface &srvInterface,
		ColumnTypes     &argTypes,
		ColumnTypes     &returnType
	) {
		LOG(srvInterface, "");

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
		LOG(srvInterface, "");

		// names and sizes of output column values
		returnType.addVarchar(32, "unload_key");
		returnType.addInt("unload_value");
	}

	virtual void
	getParameterType (
		ServerInterface  &srvInterface,
		SizedColumnTypes &parameterTypes
	) {
		LOG(srvInterface, "");

		parameterTypes.addVarchar(PATH_MAX, "outfile");
	}
};

RegisterFactory(unload_factory);
