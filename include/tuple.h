#ifndef TUPLE_H
#define TUPLE_H

#include "data.h"
#include "buffer.h"

#include <vector>
#include <cstdlib>

/** A tuple is a set of records with identical values.*/
class Tuple {
	std::vector<Data> values;    
	uint multiplicity=0;   //!< number of records in this tuple.

public:
	// Constructors
	Tuple();  //!< create empty tuple
	Tuple(const std::vector<Data>& vals, uint mult);
	/**Constructs the tuple by consuming the buffer*/
	Tuple(Buffer* B, const std::vector<Dtype>& dtypes);
	
	// Utility functions
	std::string show(int verbose=0) const;
	uint size() const;
	
	/**Splits the tuple at position pos to produce a prefix tuple of 
	size pos and its corresponding suffix. Prefix has indexes [0,pos)
	and suffix has indexes [pos,end())*/
	std::pair<Tuple, Tuple> split(uint pos);
	void write_buffer(Buffer* B);

	// getters and setter
	std::vector<Dtype> get_dtypes() const;
	Data get(int i) const;
	uint get_multiplicity();
	void set_multiplicity(uint mult);
	void add_multiplicity(uint val);


	bool operator<(const Tuple& tup) const;
	bool operator>(const Tuple& tup) const;
	bool operator==(const Tuple& tup) const;
	Tuple operator-(const Tuple& tup) const;
	Tuple operator+(const Tuple& tup) const;
};


// An interface of a stream of tuples
class TupleStream {
public:
	virtual bool has_next_tup() = 0;
	virtual Tuple next_tup() = 0;
};

#endif