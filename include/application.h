#ifndef APPLICATION_H
#define APPLICATION_H

#include "query_template.h"
#include "base_relation.h"
#include "index.h"
#include "rewriting.h"
#include "mcd.h"
#include "disk_manager.h"
#include "query_processor.h"

#include <string>
#include <vector>
#include <map>
#include <set>
#include <list>
#include <cstdlib>

#include <boost/filesystem.hpp>
#include <tsl/htrie_map.h>

/**Wrapper class for ES application. Its input is a set of query workload over a set of
base relations. It generates candidate indexes and teir MCD. Then an optimization algorithm
picks the indexes and MCDs. Finally it answer arbitrary query instances using the obtained index design*/
class Application {

	struct Design {
		std::set<const Index*> stored_indexes;
		std::map<const QueryTemplate*, Rewriting> rewritings;
		std::set<std::pair<const QueryTemplate*, uint>> rem_goals;
		float lb_cost;
		float ub_cost;
		float cost;
		uint num_rem_goals;
		uint min_mcd_pos=0;
		uint last_updated;

		Design(std::set<const Index*> inds, std::map<const QueryTemplate*, Rewriting> rwts);
		bool iscomplete() const;
		float get_priority(uint id) const;
		void add_mcd(const MCD* mcd, const std::map<std::pair<const QueryTemplate*, uint>, uint>& goal2lastpos,
			const std::map<const MCD*, uint>& mcd_to_order);
		bool operator<(const Design& dt) const;  //!< comparing priorities for the designs
		std::string show(const std::map<const MCD*, uint>& mcd_to_order) const;
	};

	const DiskManager& dm;
	boost::filesystem::path db_path;

	const std::vector<const Entity*> entities;
	const std::vector<const BaseRelation*> base_rels;
	const std::vector<const QueryTemplate*> workload;

	std::list<Index> I; //!< container for candidate indexes I
	std::list<MCD> mcds;  //!< container for candidate mcds
	std::vector<const MCD*> mcd_order; //!< the order in which MCDs can be picked
	std::map<const MCD*, uint> mcd_to_order; 
	std::map<std::pair<const QueryTemplate*, uint>, uint> goal2lastpos;   //!< goal2lastpos[g] = position of an mcd on or after which the goal g can't be covered
	std::map<const Index*, std::set<const MCD*>> M;  //!< container for candidate MCDs
	tsl::htrie_map<char, std::set<uint>> signatures;  //!< all unique signatures of MCDs and the maintenance costs of their underlying indexes

	std::vector<std::map<uint, const MCD*>> querygoal2br;  //!< for each sub-goal 'g' of i^th query, querygoal2br[i][g] = mcd of the base relation covering it
	std::list<IndexTable> tables; //!< container for tables
	std::list<IndexNavigator> navs; //!< container for navigators
	float approx_factor=0;

	void initialize_candidates(); //!< enumerates simple candidate indexes and their MCDs that contain only a single node
	
	std::set<const MCD*> inner_greedy(const Index* index, uint& negc,
		float& cost, const std::map<const QueryTemplate*, std::set<uint>>& rem_goals, uint num_rem_goals,
		const std::map<const QueryTemplate*, Rewriting>& rewritings, bool oe, uint min_mcd_pos) const;

	Design optimize_wsc(const Design& Dinit, bool oe) const;  //!< uses timeoe of oe is true and timeue otherwise

	std::vector<Design> expand(const Design* design) const;

	Design D;
public:

	static float wt_maint;
	static uint pick_fn;  //!< 0->best LB, 1->best UB, 2->least remaining goals
	static uint it_deep_k; //!< k value for iterative deepining
	static int num_expand; //!< max number of designs to return when calling expand. If num_expand=-1, return all designs 
	static int max_iters;

	Application(std::vector<const Entity*> ents, std::vector<const BaseRelation*> rels,
		std::vector<const QueryTemplate*> query_workload, const DiskManager& disk_manager,
		boost::filesystem::path pth);
	void generate_candidates(uint max_depth, float max_maint_cost); //!< generates candidate indexes with depth <= max_depth

	std::string show_candidates() const;

	std::vector<std::pair<uint, float>> optimize();

	std::string show_design() const;

	void create_indexes();

	std::vector<std::vector<int>> execute_query_instance(const QueryTemplate* query, std::map<uint,int> leaf_vals);
};



#endif