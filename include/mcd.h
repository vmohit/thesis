#ifndef MCD_H
#define MCD_H

#include "query_template.h"
#include "index.h"

#include <string>
#include <vector>
#include <map>
#include <set>

/**Mapping from the nodes of index tree to the nodes of query template*/
struct MCD {
	const Index* index;
	const QueryTemplate* query;
	std::map<uint, uint> index2query;
	std::map<uint, uint> query2index;
	std::set<uint> covered_goals;
	std::vector<uint> node2root_distances;
	float timeue;
	float timeoe=-1;
	static float disk_seek_cost;

	MCD(const Index* ind, const QueryTemplate* qt, 
		const std::map<uint, uint>& i2q);

	std::string get_signature() const;
	std::string show() const;
};

bool comp_mcd(const MCD* mcd1, const MCD* mcd2);

#endif