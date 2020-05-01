#ifndef REWRITING_H
#define REWRITING_H


#include "mcd.h"

#include <string>
#include <vector>
#include <map>
#include <tsl/htrie_map.h>
#include <set>


struct Rewriting {
	const QueryTemplate* query;
	std::map<uint, const MCD*> querygoal2brmcd; //!< maps a goal to the br MCD covering it. If some mcd in "mcds" already covers the goal g, then querygoal2brmcd[g] is NULL
	std::set<const MCD*> mcds; 
	std::map<uint, std::set<const MCD*>> querynode2mcds;  //!< maps each query node to mcds containing the node. If no such mcd exists then it maps the node to base relations convering it
	std::set<uint> covered_goals;
	float time_cost=0;  

	Rewriting();  //!< default constructor define so that a rewriting can be value for a map. Using any function on this default constructor will raise assert error 
	Rewriting(const QueryTemplate* qry, const std::map<uint, const MCD*>& goal2brmcds);

	void add_mcd(const MCD* mcd);

	bool iscomplete() const; //!< returns true if its complete

	std::string show() const;

	float timeoe(const MCD* mcd) const;  //!< overestimated time for given mcd and rewriting
};


#endif