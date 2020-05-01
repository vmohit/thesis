#include "data.h"
#include "tuple.h"
#include "index_table.h"
#include "column_group.h"
#include "buffer.h"
#include "disk_manager.h"
#include "utils.h"
#include "base_relation.h"

#include <cstdio>
#include <cassert>
#include <iostream>
#include <vector>
#include <string>
#include <queue>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/dynamic_bitset.hpp> 
#include <regex>
#include <map>
#include <set>
#include <nlohmann/json.hpp>

using namespace boost::filesystem;
using namespace boost::algorithm;
using namespace std;
using boost::filesystem::path;
using json = nlohmann::json;


void test_data();
void test_tuple();
string clean(string str);
void test_buffer();
void test_disk_manager();
void test_index_table();
class simple_document_reader;


/*----------------MAIN FUNCTION----------------*/

int main() {
	// test_data();
	// test_tuple();
	test_index_table();
	// test_buffer();
	// test_disk_manager();
	return 0;
}

/*-------------WORKING TEST FUNCTIONS--------------*/

class simple_document_reader : TupleStream {
	fs::ifstream corpus;
	string line;
	uint line_no=0;
	uint pos=0;
	queue<string> tokens;
public:
	~simple_document_reader() {
		corpus.close();
	}
	simple_document_reader(path corpus_path) {
		corpus.open(corpus_path);
		assert(corpus.is_open());
	}
	bool has_next_tup() {
		if (!tokens.empty())
			return corpus.peek() != std::ifstream::traits_type::eof();
		else
			return true;
	}

	Tuple next_tup() {
		if(tokens.empty()) {
			assert(getline(corpus, line));
			line = clean(line);
			pos=0;
			line_no++;
			vector<string> buf;
			split(buf, line, is_space());
			for(auto& tok: buf) 
				tokens.push(tok);
		}
		Tuple tup(vector<Data>{Data(tokens.front()), Data(line_no), Data(pos)}, 1);
		tokens.pop();
		pos+=1;
		return tup;
	}
};


void test_data() {
	Data id(4328);
	cout<<id.show()<<endl;

	Data sd("Hellow World");
	cout<<sd.show()<<endl;
}

void test_tuple() {
	Buffer B;
	vector<Tuple> data;
	data.push_back(Tuple(vector<Data>({Data("abc"), Data(1)}),1));
	data.push_back(Tuple(vector<Data>({Data("abd"), Data(10)}),1));
	data.push_back(Tuple(vector<Data>({Data("bde"), Data(6)}),1));
	
	data[0].write_buffer(&B);
	for(uint i=1; i<data.size(); i++)
		(data[i]-data[i-1]).write_buffer(&B);

	Tuple curr_tup(&B, data[0].get_dtypes());
	cout<<curr_tup.show()<<endl;
	for(uint i=1; i<data.size(); i++){
		Tuple diff = Tuple(&B, data[0].get_dtypes());
		cout<<diff.show()<<endl;
		curr_tup = curr_tup+diff;
		cout<<curr_tup.show()<<endl;
	}
}

string clean(string str) {
	trim(str);
	regex re("\\s{2,}");
	return regex_replace(str, re, " ");
}


void test_buffer() {
	Buffer B;
	for(int i=0; i<10; i++)
		B.push(i, (i%2==0? Code_scheme::gamma : Code_scheme::plain));
	int i=0;
	cout<<B.size()<<endl;
	while(i<5){
		cout<<B.pop_front((i%2==0? Code_scheme::gamma : Code_scheme::plain))<<endl;
		i++;
	}
	Buffer B2;
	B2.push(B.flush());
	while(i<10){
		cout<<B2.pop_front((i%2==0? Code_scheme::gamma : Code_scheme::plain))<<endl;
		i++;
	}
	cout<<B2.size()<<endl;
}

void test_disk_manager() {

	DiskManager dm("temp");

	dm.delete_dir("test_dir", true);
	cout<< dm.mkdir_ifnotexist("test_dir") <<endl;
	dm.create_file(path("test_dir/INV_test.txt"));
	dm.create_file(path("test_dir/INV_test1.bin"));
	dm.create_file(path("test_dir/INV_test2.bin"));
	dm.create_file(path("test_dir/IN.txt"));
	dm.create_file(path("test_dir/IN.bin"));
	path txt_file("test_dir/IN.txt");
	path bin_file("test_dir/IN.bin");
	
	dm.delete_file_startingwith(path("test_dir"), "INV");
	json j = {
	  {"pi", 3.141},
	  {"happy", true},
	  {"name", "Niels"},
	  {"nothing", nullptr},
	  {"answer", {
	    {"everything", 42}
	  }},
	  {"list", {1, 0, 2}},
	  {"object", {
	    {"currency", "USD"},
	    {"value", 42.99}
	  }}
	};

	dm.write(txt_file, j.dump(4));
	printf("\n\n\n %s \n\n\n", dm.slurp(path(txt_file)).c_str());

	Buffer B;
	vector<Tuple> data = {
		Tuple(vector<Data>{Data("str1"), Data(-1), Data(1111)}, 1),
		Tuple(vector<Data>{Data("str22"), Data(-2), Data(2222)}, 1),
		Tuple(vector<Data>{Data("str333"), Data(-3), Data(3333)}, 1),
		Tuple(vector<Data>{Data("str4444"), Data(-4), Data(4444)}, 1)
	};

	data[0].write_buffer(&B);
	auto b_flush = B.flush();
	dm.append(bin_file, b_flush);
	cout<<b_flush.size() <<" "<<dm.file_size(bin_file)<<endl;
	uint offset = b_flush.size();

	data[1].write_buffer(&B);
	data[2].write_buffer(&B);
	b_flush = B.flush();
	auto actual_data = b_flush;
	uint block_size = b_flush.size();
	dm.append(bin_file, b_flush);

	data[3].write_buffer(&B);
	b_flush = B.flush();
	dm.append(bin_file, b_flush);

	auto obtained_data = dm.seek_and_read(bin_file, offset, block_size);
	assert(actual_data.size()==obtained_data.size());
	cout<<actual_data.size()<<endl;
	for(uint i=0; i<actual_data.size(); i++) {
		if(actual_data[i]!=obtained_data[i]) {
			cout<<i<<endl;
			assert(false);
		}
	}

	Buffer newB;
	newB.push(dm.seek_and_read(bin_file, offset, block_size));
	while(newB.size()!=0) {
		cout<<Tuple(&newB, data[0].get_dtypes()).show()<<endl;
	}

	// testing connection class
	path bin_file2("test_dir/IN2.bin");
	dm.create_file(bin_file2);
	B.flush();
	auto conn = dm.get_connection(bin_file2);
	data[0].write_buffer(&B);
	auto bytes = B.flush();
	uint bsize = bytes.size();
	conn->append(bytes);
	data[1].write_buffer(&B);
	bytes = B.flush();
	bsize += bytes.size();
	conn->append(bytes);
	data[2].write_buffer(&B);
	bytes = B.flush();
	bsize += bytes.size();
	conn->append(bytes);
	conn->close();

	cout<<"--------\n";
	B.push(dm.seek_and_read(bin_file2, 0, bsize));
	while(B.size()!=0) {
		cout<<Tuple(&B, data[0].get_dtypes()).show()<<endl;
	}

	dm.delete_dir("test_dir");
}

void test_index_table() {

	// using a temporary directory to make the errors in experiments harmless
	DiskManager dm("temp");

	string corpus_name = "toy_dataset0";
	string db_name = "toy_dataset0";

	path db_path(db_name);

	IndexTable inv_index("INV", vector<Dtype>{Dtype::String, Dtype::Int, Dtype::Int}, 
		vector<vector<string> >{{"k"}, {"d","p"}}, 
		db_path / "indexes", dm);

	path corpus_path = path("data") / corpus_name / "corpus.txt";

	simple_document_reader reader(corpus_path);

	while(reader.has_next_tup()) {
		inv_index.add_tuple(reader.next_tup());
	}
	inv_index.freeze();

	printf("------START---------------\n");
	IndexNavigator inv_nav(&inv_index);
	for(auto& dt: inv_nav.scan()) {
		cout<<dt.show().c_str();
		auto inv_d = inv_nav.look_up(dt.get_str_val()).second;
		for(auto& dt_d: inv_d.scan()) {
			cout<<",\t"<<dt_d.show().c_str();
			auto inv_p = inv_d.look_up(dt_d.get_int_val()).second;
			for(auto& dt_p: inv_p.dump())
				cout<<",\t"<<dt_p.first.show().c_str()<<" mult="<<dt_p.second<<endl;
		}
	}
	printf("------END---------------\n");
}

