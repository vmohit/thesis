#include "utils.h"

#include <ctime>
#include <vector>
#include <cstdlib>
#include <string>
#include <cmath>
#include <algorithm>
#include <random>
#include <cassert>
#include <set>

using std::set;
using std::string;
using std::vector;

bool esutils::remove_char(char c) {
	if(c>='a' && c<='z')
		return false;
	if(c>='A' && c<='Z')
		return false;
	if(c=='_' || c=='(' || c==')' || c==',')
		return false;
	return true;
}

/**removes all characters except "a-z", "A-Z", "(", ")", "," and "_" */
string esutils::clean_br(string str) {
	str.erase(std::remove_if(str.begin(), str.end(), esutils::remove_char), str.end());
	return str;
}

vector<string> esutils::split(const string& str, 
	const string& delim) {
	
	vector<string> result;
	uint start = 0;
	auto end = str.find(delim);
	while (end != std::string::npos) {
		result.push_back(str.substr(start, end - start));
		start = end + delim.length();
		end = str.find(delim, start);
	}
	result.push_back(str.substr(start, end));
	return result;
}

vector<int> esutils::range(uint n) {
	vector<int> result;
	for(uint i=0; i<n; i++)
		result.push_back((int) i);
	return result;
}

vector<float> esutils::normalize(const vector<float>& dist) {
	float norm_factor=0;
	for(auto prob: dist) {
		assert(prob>=0);
		norm_factor+=prob;
	}
	assert(norm_factor>=0.00001);
	vector<float> result = dist;
	for(uint i=0; i<result.size(); i++)
		result[i] /= norm_factor;
	return result;
}


float esutils::apowb(float a, float b) {
	return exp(b*log(a));
}

esutils::random_number_generator::random_number_generator() :
	distribution(0.0, 1.0) {}

uint esutils::random_number_generator::sample(const vector<float>& dist) {
	assert(dist.size()!=0);
	float sum=0;
	for(auto prob: dist) {
		assert(prob>=0);
		sum+=prob;
	}
	assert(abs(sum-1)<0.000001);
	float r = distribution(generator);
	float curr=0;
	for(uint i=0; i<dist.size(); i++) {
		if(curr+dist[i]>=r)
			return i;
		curr += dist[i];
	}
	return dist.size()-1;
}

set<uint> esutils::set_difference(const set<uint>& s1, const set<uint>& s2) {
	set<uint> result;
	for(auto ele: s1)
		if(s2.find(ele)==s2.end())
			result.insert(ele);
	return result;
}

set<uint> esutils::set_intersection(const set<uint>& s1, const set<uint>& s2) {
	set<uint> result;
	for(auto ele: s1)
		if(s2.find(ele)!=s2.end())
			result.insert(ele);
	return result;
}

set<int> esutils::set_intersection(const set<int>& s1, const set<int>& s2) {
	set<int> result;
	for(auto ele: s1)
		if(s2.find(ele)!=s2.end())
			result.insert(ele);
	return result;
}

uint esutils::set_intersection_size(const set<uint>& s1, const set<uint>& s2) {
	uint result=0;
	for(auto ele: s1)
		if(s2.find(ele)!=s2.end())
			result++;
	return result;
}

void esutils::set_difference_inplace(set<uint>& s1, const set<uint>& s2) {
	for(auto ele: s2)
		s1.erase(ele);
}
