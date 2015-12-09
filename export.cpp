#include <time.h>
#include <pthread.h>
#include <limits.h> // NAME_MAX
#include <fcntl.h>  // open
#include <unistd.h> // close
#include <string.h>

#include "Vertica.h"

// as per https://my.vertica.com/docs/7.1.x/HTML/index.htm#Authoring/ExtendingHPVertica/UDx/CreatingAPolymorphicUDF.htm
// number of arguments of the function, ie. number columns
#define MAX_COLUMNS 1600

#define FIELD_DELIMITER ','
#define RECORD_DELIMITER '\n'

#define NUMERIC_BUFFER_SIZE 1024 // since numeric can have scalu/precision up to 1024
#define    DATE_BUFFER_SIZE 16

#define EXPORT_BUFFER_SIZE (32*1024)
#define EXPORT_BUFFER_HWM ((int)(EXPORT_BUFFER_SIZE*0.8))

using namespace Vertica;
using namespace std;

class Export;
typedef void (Export::*col_writer_func_t)(ServerInterface&, BlockReader&, size_t);

pthread_mutex_t setup_mutex  = PTHREAD_MUTEX_INITIALIZER;
int             thread_count = 0;
bool            setup_done   = false;

pthread_mutex_t process_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t write_mutex   = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t destroy_mutex = PTHREAD_MUTEX_INITIALIZER;
bool            destroy_done  = false;

size_t            num_cols;
col_writer_func_t col_writer[MAX_COLUMNS];
char              fpath     [NAME_MAX];
int               fd;

class Export : public ScalarFunction
{

private:

int          tid;
char         temp_numeric[NUMERIC_BUFFER_SIZE+1]; // extra char for \0
char         temp_date   [   DATE_BUFFER_SIZE+1];
struct tm    temp_tm;
time_t       temp_epoch;
char        *export_buffer;
size_t       export_size;
off_t        export_offset;
off_t        export_last_record_end_offset;
ServerInterface *srv_if;

void write_buffer()
{
	this->srv_if->log("method=write_buffer tid=%u last_record_end_offset=%lu offset=%lu", this->tid, this->export_last_record_end_offset, this->export_offset);
	pthread_mutex_lock(&write_mutex);
	// TODO check num of bytes written
	size_t len = write(fd, this->export_buffer, this->export_last_record_end_offset);
	pthread_mutex_unlock(&write_mutex);

	this->srv_if->log("method=write_buffer tid=%u bytes_wrote=%lu", this->tid, len);
	memcpy(
		this->export_buffer,
		this->export_buffer + this->export_last_record_end_offset, 
		this->export_offset - this->export_last_record_end_offset
	);

	this->export_offset = 0;
	this->export_last_record_end_offset = 0;
}

void write_int(ServerInterface &srv_if, BlockReader &br, size_t i)
{
	int len;
	len = sprintf(
		this->export_buffer + this->export_offset,
		"%llu",
		*br.getIntPtr(i)
	);
	this->export_offset += len;

	if (this->export_offset > EXPORT_BUFFER_HWM)
	{
		write_buffer();
	}
}

void write_numeric(ServerInterface &srv_if, BlockReader &br, size_t i)
{
	br.getNumericRef(i).toString(this->export_buffer + this->export_offset, NUMERIC_BUFFER_SIZE);
	size_t len = strlen(this->export_buffer + this->export_offset);
	this->export_offset += len;

	if (this->export_offset > EXPORT_BUFFER_HWM)
	{
		write_buffer();
	}
}

void write_float(ServerInterface &srv_if, BlockReader &br, size_t i)
{
	int len;
	len = sprintf(
		this->export_buffer + this->export_offset,
		"%lf",
		*br.getFloatPtr(i)
	);
	this->export_offset += len;

	if (this->export_offset > EXPORT_BUFFER_HWM)
	{
		write_buffer();
	}
}

void write_varchar(ServerInterface &srv_if, BlockReader &br, size_t i)
{
	//this->fh << br.getStringRef(i).str();
}

void write_date(ServerInterface &srv_if, BlockReader &br, size_t i)
{
	// $(VERTICA_SDK)/include/TimestampUDxShared.h
	// DateADT represent number of days since 2000-01-01
	// including backward dates

	//     86400 = # of seconds per day
	// 946684800 = unix epoch of 2000-01-01

	// get epoch of the date
	this->temp_epoch = (time_t)br.getDateRef(i)*86400+946684800;

	// extract date fragments, format it, print it out
	localtime_r(&this->temp_epoch, &this->temp_tm);

	size_t len;
	len = strftime(this->export_buffer + this->export_offset, DATE_BUFFER_SIZE, "%Y-%m-%d", &this->temp_tm);
	this->export_offset += len;

	if (this->export_offset > EXPORT_BUFFER_HWM)
	{
		write_buffer();
	}
}

public:

virtual void
setup (
	ServerInterface &srvInterface,
	const SizedColumnTypes &argTypes
) {
	srvInterface.log("method=setup pthread=%lx action=start", pthread_self());

	// {setup_mutex
	pthread_mutex_lock(&setup_mutex);

	// assign human readable "thread ID"
	this->tid = ++thread_count;
	srvInterface.log("method=setup tid=%u action=tid_assigned", this->tid);

	if (!setup_done)
	{
		srvInterface.log("method=setup tid=%u action=configure_writers", this->tid);

		ParamReader paramReader(srvInterface.getParamReader());
		if (!paramReader.containsParameter("fpath"))
			vt_report_error(0, "Missing fpath parameter.");

		strncpy(fpath, paramReader.getStringRef("fpath").str().c_str(), NAME_MAX);

		fd = open(fpath, O_CREAT|O_WRONLY|O_TRUNC|O_EXCL, 0666 );
		if ( -1 == fd )
		{
			vt_report_error(0, "Failed to open file [%s] for writing: %s",
				fpath,
				strerror(errno)
			);
		}

		num_cols = argTypes.getColumnCount();
		for (size_t i = 0; i < num_cols; i++)
		{
			const VerticaType col_type = argTypes.getColumnType(i);
			if (col_type.isInt())
			{
				srvInterface.log("INT");
				col_writer[i] = &Export::write_int;
			}
			else if (col_type.isNumeric())
			{
				srvInterface.log("NUMERIC");
				col_writer[i] = &Export::write_numeric;
			}
			else if (col_type.isFloat())
			{
				srvInterface.log("FLOAT");
				col_writer[i] = &Export::write_float;
			}
			else if (col_type.isVarchar())
			{
				srvInterface.log("VARCHAR");
				col_writer[i] = &Export::write_varchar;
			}
			else if (col_type.isDate())
			{
				srvInterface.log("DATE");
				col_writer[i] = &Export::write_date;
			}
			else
				vt_report_error(0, "Unsupported export type.");
		} // for


		setup_done = true;

	} // if (!setup_done)
	else
	{
		srvInterface.log("method=setup tid=%u action=writers_already_configured", this->tid);
	} // if (!setup_done

	pthread_mutex_unlock(&setup_mutex);
	// setup_mutex}

	// per-thread initialization
	this->export_buffer = (char *)malloc(EXPORT_BUFFER_SIZE);
	this->export_offset = 0;
	this->export_last_record_end_offset = 0;

	this->srv_if = &srvInterface;


	return;
}

virtual void
destroy (ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
{
	srvInterface.log("method=destroy tid=%u", this->tid);

	// {destroy_mutex
	pthread_mutex_lock(&destroy_mutex);
	if (!destroy_done)
	{
		srvInterface.log("method=destroy tid=%u action=close_file", this->tid);
		close(fd);

		destroy_done = true;

		// reset thread_count
		thread_count = 0;
	}
	else // if (!destroy_done)
	{
		srvInterface.log("method=destroy tid=%u action=file_already_closed", this->tid);
	} // if (!destroy_done)

	pthread_mutex_unlock(&destroy_mutex);
	// destroy_mutex}

	free(this->export_buffer);
}

virtual void
processBlock (ServerInterface &srvInterface, BlockReader &arg_reader, BlockWriter &res_writer)
{
	//srvInterface.log("processBlock, thread %u", this->tid);
	//

	long rows_processed = 0;
	do {
		size_t i;
		for (i = 0; i < num_cols; i++)
		{
			if (!arg_reader.isNull(i)) {
				(this->*col_writer[i])(srvInterface, arg_reader, i);
			}

			// write field delimiter
			this->export_buffer[ this->export_offset++ ] = FIELD_DELIMITER;
		}
		this->export_buffer[ this->export_offset - 1 ] = RECORD_DELIMITER;
		this->export_last_record_end_offset = this->export_offset;

		res_writer.setInt(1);
		res_writer.next();
		rows_processed += 1;
	} while (arg_reader.next());

	if (this->export_offset > 0)
	{
		write_buffer();
	}

	srvInterface.log("method=process tid=%u rows_processed=%lu", this->tid, rows_processed);
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
	ColumnTypes     &returnType
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

RegisterLibrary(
	"Jan Holcapek <holcapek@gmail.com>"
,	"library_build_tag"
,	"library_version"
,	"library_sdk_version"
,	"source_url"
,	"description"
,	"" // intentionally empty
,	"signature"
);
