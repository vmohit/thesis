#ifndef UTILS_H
#define UTILS_H

#include <vector>
#include <string>
#include <random>
#include <set>
#include <map>

namespace esutils {

	bool remove_char(char c);

	/**removes all characters except "a-z", "A-Z", "(", ")", "," and "_" */
	std::string clean_br(std::string str);

	std::vector<std::string> split(const std::string& str, 
		const std::string& delim); 

	std::vector<int> range(uint n);

	float apowb(float a, float b);
	std::vector<float> normalize(const std::vector<float>& dist);

	class random_number_generator {
		std::default_random_engine generator;
		std::uniform_real_distribution<float> distribution;
	public:
		random_number_generator();
		uint sample(const std::vector<float>& dist);  //!< sample from a given multinomial distribution
	};

	// set operations
	std::set<uint> set_intersection(const std::set<uint>& s1, const std::set<uint>& s2);
	std::set<int> set_intersection(const std::set<int>& s1, const std::set<int>& s2);
	std::set<uint> set_difference(const std::set<uint>& s1, const std::set<uint>& s2);
	uint set_intersection_size(const std::set<uint>& s1, const std::set<uint>& s2);
	void set_difference_inplace(std::set<uint>& s1, const std::set<uint>& s2);
}

#endif