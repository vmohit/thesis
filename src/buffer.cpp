#include <buffer.h>

#include <vector>
#include <boost/dynamic_bitset.hpp> 
#include <cstdlib>

using std::vector;
using boost::dynamic_bitset;

#include <iostream>
using std::cout;
using std::endl;

/**appends the contents of b to the reference of a*/
void Buffer::concat(dynamic_bitset<>& a,
	dynamic_bitset<> b) {
	int n=a.size();
	a.resize(a.size()+b.size());
	for(uint i=0; i<b.size(); i++)
		a[n+i] = b[i];
}

/**Converts the argument val to a bitset of variable size*/
dynamic_bitset<> Buffer::gamma_encode(uint32_t val) {

	uint size=0;  //!< number of bits needed to represent val
	uint num = val;
	while(num!=0) {
		size+=1;
		num = num>>1;
	}
	dynamic_bitset<> result(size, val);
	dynamic_bitset<> prefix(size);
	prefix.set();
	prefix.push_back(false);
	dynamic_bitset<> code_bits;
	code_bits.push_back(true); code_bits.push_back(false);
	concat(code_bits, prefix);
	concat(code_bits, result);
	return code_bits;
}

/**Converts the argument val to a bitset of size 8*/
dynamic_bitset<> Buffer::plain_encode(uint32_t val) {
	assert(val<256); 
	dynamic_bitset<> code_bits;
	code_bits.push_back(true); code_bits.push_back(true); 
	concat(code_bits, dynamic_bitset<>(8,val));
	return code_bits;
}

Buffer::Buffer() {
	pos=0;
}

void Buffer::push(uint32_t value, Code_scheme sch /*=Code_scheme::gamma*/) {
	//cout<<value<<" "<<(sch==Code_scheme::gamma ? "gamma" : "plain")<<endl;
	dynamic_bitset<> newbs;
	if(sch == Code_scheme::gamma)
		newbs = gamma_encode(value);
	else if(sch == Code_scheme::plain)
		newbs = plain_encode(value);
	else
		perror("Unsupported coding scheme");

	concat(buffer, newbs);
}

std::vector<char> Buffer::flush() {
	vector<char> result;
	uint N=buffer.size()-pos; //!< number of unread bits
	buffer.resize(pos + N + (N%8==0? 0: 8-(N%8))); //!< padding 0 bits to make the number of unread bits a multiple of 8
	for(; pos<buffer.size(); pos+=8) {
		uint val=0;
		for(uint j=0; j<8; j++)
			val += (1<<j)*buffer[pos+j];
		result.push_back((char) val);
	}
	buffer.resize(0);
	pos=0;
	return result;
}

void Buffer::push(const std::vector<char>& data) {
	uint N=buffer.size()-pos; //!< number of unread bits
	buffer <<= pos;  //!< dropping first pos bits that are already consumed
	buffer.resize(N);
	pos = 0;
	for(auto& c: data){
		uint t=c;
		for(uint i=0; i<8; i++)
			buffer.push_back(((1<<i)&t)!=0);
	}
}

Code_scheme Buffer::parse_code_scheme() {
	while(pos<buffer.size())
		if(buffer[pos])
			break;
		else
			pos++;
	assert(pos+1<buffer.size());
	pos++;
	if(buffer[pos++])
		return Code_scheme::plain;
	else
		return Code_scheme::gamma;
}

uint32_t Buffer::pop_front(Code_scheme sch /*=Code_scheme::gamma*/) {
	Code_scheme sch2 = parse_code_scheme();
	assert(sch2==sch);
	uint32_t answer=0;
	if(sch==Code_scheme::gamma) {
		uint ptr = pos;
		while(ptr<buffer.size()) 
			if(buffer[ptr])
				ptr++;
			else
				break;
		int size = ptr-pos;
		assert(ptr+size+1 <= buffer.size());
		ptr++;
		for(int i=0; i<size; i++) {
			answer += buffer[ptr]*(1<<i);
			ptr++;
		}
		pos = ptr;
	}
	else if(sch == Code_scheme::plain) {
		assert(pos+8<=buffer.size());
		for(int i=0; i<8; i++)
			answer += buffer[pos++]*(1<<i);
	}
	else 
		perror("Unsupported coding scheme");
	return answer;
}

uint Buffer::size() {
	while(pos<buffer.size())
		if(buffer[pos])
			break;
		else
			pos++;
	return buffer.size()-pos;
}



