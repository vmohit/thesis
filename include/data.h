#ifndef DATA_H
#define DATA_H

#include "buffer.h"

#include <string>
#include <vector>

// --------------------------------------------------------------------------

/** Data types supported by the system. */
enum class Dtype { String, Int }; 

std::string dtype_to_str(Dtype dtype);

/** Class for storing cell data within an index table. Datatypes string and int are supported */
class Data {
	Dtype dtype;
	std::string str_val;
	int int_val;  //!< it is the number of back spaces in case data type is string
	static int start_c;

	Data(const std::string& val, uint bksp);
public:
	// Constructors
	Data(const std::string& val);
	Data(const int& val);
	/**Constructs Data by consuming from the buffer*/
	Data(Buffer* B, Dtype dtp);

	// Utility functions
	std::string show(int verbose=0) const;
	bool isdiff() const;  //!< returns true if its a diff string
	void write_buffer(Buffer* B);

	// getters and setters
	Dtype get_dtype() const;
	int get_int_val() const;
	std::string get_str_val() const;

	// Operators
	bool operator!=(const Data& dt) const;
	bool operator<(const Data& dt) const;
	bool operator==(const Data& dt) const;
	Data operator-(const Data& dt) const;
	Data operator+(const Data& dt) const;
};

#endif