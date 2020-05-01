#ifndef QUERYPROCESSOR_H
#define QUERYPROCESSOR_H

#include "rewriting.h"
#include "query_template.h"
#include "index_table.h"
#include "disk_manager.h"

#include <vector>
#include <map>
#include <string>
#include <set>
#include <boost/filesystem.hpp>

struct Dataframe {
	const QueryTemplate* query;
	std::vector<uint> cols;
	std::vector<std::vector<int>> values;
	Dataframe(const QueryTemplate* qry, std::vector<uint> columns);
	std::vector<std::vector<int>> reorder_cols() const;
	std::string show() const;
};


std::vector<std::vector<int>> execute_instance(const Rewriting* R, std::map<uint, int> values);

void add_tuples(IndexTable& table, const Index* index, std::map<uint, std::set<int>>& doms, 
	const std::vector<uint>& node_order, std::vector<int>& values);

IndexTable create_index_table(const Index* index, std::string name, boost::filesystem::path db_path,
	const DiskManager& dm);

#endif