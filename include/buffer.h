#ifndef BUFFER_H
#define BUFFER_H

#include <vector>
#include <cstdlib>
#include <string>
#include <boost/dynamic_bitset.hpp> 


/**Currently the encodings "gamma" and "plain" are supported. */
enum class Code_scheme { gamma, plain }; 

/**A queue of bits that can push and pop 32 bit integers using  different coding schemes.  */
class Buffer {
	boost::dynamic_bitset<> buffer;
	uint pos;

	// private utility functions
	void concat(boost::dynamic_bitset<>& a, boost::dynamic_bitset<> b);
	boost::dynamic_bitset<> gamma_encode(uint32_t val);
	boost::dynamic_bitset<> plain_encode(uint32_t val);
	Code_scheme parse_code_scheme();
public:
	Buffer();
	
	void push(uint32_t value, Code_scheme sch=Code_scheme::gamma);  //!< pushes the value into the buffer using specified coding
	std::vector<char> flush(); //!< transfer all unread bits into a char array with padded 0 bits to be stored in disk
	uint32_t pop_front(Code_scheme sch=Code_scheme::gamma);  //!< reads the integer at the front using specified coding
	void push(const std::vector<char>& data);  //!< pushes the char array at once
	uint size();
};


#endif