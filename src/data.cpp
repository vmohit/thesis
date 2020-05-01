#include "data.h"
#include "buffer.h"

#include <string>
#include <vector>
#include <cstdlib>
#include <cstdio>

using std::string;
using std::vector;
using std::to_string;


int Data::start_c=32;

// Constructors

Data::Data(const std::string& val) {
	str_val = val;
	dtype = Dtype::String;
	int_val = 0;
}

Data::Data(const std::string& val, uint bksp) {
	str_val = val;
	dtype = Dtype::String;
	int_val = bksp;
}

Data::Data(const int& val) {
	int_val = val;
	dtype = Dtype::Int;
}

Data::Data(Buffer* B, Dtype dtp) {
	assert(B!=NULL);
	dtype = dtp;
	if(dtype==Dtype::Int) {
		uint flag = B->pop_front();
		int_val = B->pop_front();
		if(flag==1)
			int_val*=(-1);
		else if(flag!=0) {
			perror(("Unsupported value flag=" +to_string(flag)+ " of int_val").c_str());
		}
	}
	else if(dtype==Dtype::String) {
		int_val = B->pop_front();
		uint n = B->pop_front();
		str_val="";
		for(uint i=0; i<n; i++)
			str_val.push_back((char) B->pop_front(Code_scheme::plain));
	}
	else
		perror("Unsupported datatype");
}

// Utility functions

string dtype_to_str(Dtype dtype) {
	switch (dtype) { 
	case Dtype::String: 
		return "String";
		break; 
	case Dtype::Int: 
		return "Int";
		break;
	default:
		perror("Unsupported data type");
		return "";
	}
}

string Data::show(int verbose /*=0*/) const{
	string result = "";
	switch (dtype) { 
	case Dtype::String: 
		result.append("String[bs="+to_string(int_val)+"]("+str_val+")");
		break; 
	case Dtype::Int: 
		result.append("Int("+to_string(int_val)+")");
		break;
	default:
		perror("ERROR: unsupported datatype");   
	}
	return result;
}

bool Data::isdiff() const{
	if(dtype==Dtype::String)
		return int_val>0;
	return false;
}

void Data::write_buffer(Buffer* B) {
	assert(B!=NULL);
	if(dtype==Dtype::Int) {
		if(int_val<0)
			B->push(1);
		else
			B->push(0);
		B->push(abs(int_val));
	}
	else {
		B->push(int_val);
		B->push(str_val.size());
		for(char c: str_val)
			B->push((int) c, Code_scheme::plain);
	}
}


// getters and setters

Dtype Data::get_dtype() const{
	return dtype;
}

int Data::get_int_val() const{
	return int_val;
}

string Data::get_str_val() const{
	return str_val;
}

// operators

bool Data::operator!=(const Data& dt) const {
	if(dtype!=dt.dtype)
		return true;
	else
		if(dtype==Dtype::Int)
			return int_val!=dt.int_val;
		else
			return ((int_val!=dt.int_val) || (str_val.compare(dt.str_val)!=0));
}

/**Shorter tuples are smaller and Strings are smaller than ints.*/
bool Data::operator<(const Data& dt) const {
	if(dtype!=dt.dtype)
		return (dtype==Dtype::String ? true : false);
	else
		if(dtype==Dtype::Int)
			return int_val<dt.int_val;
		else
			return (str_val.compare(dt.str_val)<0);
}

/** 'abc'-'axyz' = (bs=3, bc) */
Data Data::operator-(const Data& dt) const {
	assert(dtype == dt.dtype);
	if(dtype==Dtype::Int) 
		return Data(int_val-dt.int_val);
	else {
		assert(int_val==0 && dt.int_val==0);
		uint n=0;
		while(n<str_val.size() && n<dt.str_val.size())
			if(str_val[n]==dt.str_val[n])
				n++;
			else
				break;
		return Data(str_val.substr(n,str_val.size()-n), dt.str_val.size()-n);
	}
}

/** 'axyz' + ('abc'-'axyz') = 'axyz' + (bs=3, bc) = 'abc' */
Data Data::operator+(const Data& dt) const {
	assert(dtype == dt.dtype);
	if(dtype==Dtype::Int) 
		return Data(int_val+dt.int_val);
	else {
		assert(int_val==0);
		return Data(str_val.substr(0, str_val.size()-dt.int_val)+dt.str_val);
	}
}

bool Data::operator==(const Data& dt) const {
	return !(this->operator!=(dt));
}