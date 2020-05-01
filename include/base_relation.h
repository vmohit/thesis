#ifndef BASERELATION_H
#define BASERELATION_H

#include "index_table.h"
#include <vector>
#include <cstdlib>
#include <string>


/** Entities in ER-diagram. Eg:- keyword, document, etc. */
struct Entity {
	const std::string name;  
	const uint cardinality;
	const std::vector<int> values;
	Entity(std::string nm, uint card, std::vector<int> vals);
};

/** A directional binary base relation in the corpus*/
struct BaseRelation {
	const std::string name;
	const Entity* left_ent;
	const Entity* right_ent;
	const IndexTable* tab;   
	const float n_rgl;   //!< number of unique values of right entity associated to a given value of left entity, on average
	const float n_lgr;   
	
	BaseRelation(std::string nm, const Entity& le, const Entity& re, 
		const IndexTable* table, float nrgl, float nlgr);
};

#endif