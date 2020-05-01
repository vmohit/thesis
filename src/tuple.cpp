#include "data.h"
#include "tuple.h"
#include "buffer.h"

#include <string>
#include <vector>
#include <cstdlib>
#include <cassert>

using std::string;
using std::vector;
using std::to_string;
using std::pair;
using std::make_pair;


// Constructors

Tuple::Tuple(const vector<Data>& vals, uint mult) {
	values = vals;
	multiplicity = mult;
}

Tuple::Tuple() {
	values = vector<Data>(0, Data(1));
	multiplicity=0;
}

Tuple::Tuple(Buffer* B, const std::vector<Dtype>& dtypes) {
	assert(B!=NULL);
	multiplicity = 0;
	values = vector<Data>(0, Data(1));
	for(uint i=0; i<dtypes.size(); i++)
		values.push_back(Data(B, dtypes[i]));
}

// Utility functions

pair<Tuple, Tuple> Tuple::split(uint pos) {
	assert(pos <= values.size()); 
	return make_pair(Tuple(vector<Data>(values.begin(),values.begin()+pos), multiplicity),
		Tuple(vector<Data>(values.begin()+pos,values.end()), multiplicity));
}

uint Tuple::size() const {
	return values.size();
}

string Tuple::show(int verbose /*=0*/) const {
	assert(values.size()>0);
	string result="Tuple["+to_string(multiplicity)+"](";
	result.append(values[0].show(verbose));
	for(uint i=1; i<values.size(); i++) {
		result.append(" | "+values[i].show(verbose));
	}
	return result+")";
}

// getters and setters

vector<Dtype> Tuple::get_dtypes() const {
	vector<Dtype> dtypes;
	for(auto& val: values) {
		dtypes.push_back(val.get_dtype());
	}
	return dtypes;
}

Data Tuple::get(int i) const{
	return values[i];
}

uint Tuple::get_multiplicity() {
	return multiplicity;
}

void Tuple::set_multiplicity(uint mult) {
	multiplicity = mult;
}

void Tuple::add_multiplicity(uint val) {
	multiplicity+=val;
}

void Tuple::write_buffer(Buffer* B) {
	assert(B!=NULL);
	// B->push(multiplicity);
	for(uint i=0; i<values.size(); i++)
		values[i].write_buffer(B);
}

// Operators

/** Shorter tuples are smaller and int is smaller than string*/
bool Tuple::operator<(const Tuple& tup) const {
	if(values.size()!=tup.values.size()) 
		return values.size()<tup.values.size();
	for(uint i=0; i<values.size(); i++) {
		if(values[i]!=tup.values[i])
			return values[i]<tup.values[i];
	}
	return false;
}

bool Tuple::operator>(const Tuple& tup) const {
	return tup < (*this);
}

bool Tuple::operator==(const Tuple& tup) const {
	return (!(this->operator<(tup)) && !(tup<(*this)));
}

/**Performs element-wise subtraction and preserves the multiplicity*/
Tuple Tuple::operator-(const Tuple& tup) const {
	assert(values.size()!=0); 
	if(tup.size()==0)
		return *this;
	assert(values.size()==tup.values.size());
	vector<Data> result;
	for(uint i=0; i<values.size(); i++)
		result.push_back(values[i]-tup.values[i]);
	return Tuple(result, multiplicity); 
}

Tuple Tuple::operator+(const Tuple& tup) const {
	assert(values.size()!=0); 
	if(tup.size()==0)
		return *this;
	assert(values.size()==tup.values.size());
	vector<Data> result;
	for(uint i=0; i<values.size(); i++)
		result.push_back(values[i]+tup.values[i]);
	return Tuple(result, tup.multiplicity); 
}