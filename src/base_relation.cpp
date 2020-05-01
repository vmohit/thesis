#include "base_relation.h"
#include "utils.h"
#include "data.h"

#include <string>
#include <vector>
#include <cstdlib>
#include <cassert>

using std::string;
using std::vector;
using std::to_string;
using std::pair;



Entity::Entity(string nm, uint card, vector<int> vals) : 
	name(nm), cardinality(card), values(vals) {}

BaseRelation::BaseRelation(std::string nm, const Entity& le, const Entity& re, 
		const IndexTable* table, float nrgl, float nlgr) : name(nm), left_ent(&le), right_ent(&re),
		tab(table), n_rgl(nrgl), n_lgr(nlgr){
	assert(table != NULL);
	assert(table->get_dtypes().size()==2);
	for(auto dtype: table->get_dtypes())
		assert(dtype == Dtype::Int);
}
