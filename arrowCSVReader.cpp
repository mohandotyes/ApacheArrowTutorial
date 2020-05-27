#include <iostream>
#include <vector>
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/file.h>
#include <string>
#include <tuple>
#include <arrow/stl.h>
#include <algorithm>
#include <arrow/util/iterator.h>
#include <arrow/io/api.h>
#include <arrow/compute/kernel.h>


int main()
{
   	arrow::MemoryPool* pool = arrow::default_memory_pool();
   	std::shared_ptr<arrow::io::InputStream> input;
	std::string csv_file = "yellow_tripdata_2019-01.csv";

	auto cvsFile = arrow::io::ReadableFile::Open(csv_file);
	if(cvsFile.ok()){
		input = std::move(cvsFile).ValueOrDie();

   		auto read_options = arrow::csv::ReadOptions::Defaults();
   		auto parse_options = arrow::csv::ParseOptions::Defaults();
  		auto convert_options = arrow::csv::ConvertOptions::Defaults();

   	// Instantiate TableReader from input stream and options
		auto reader = arrow::csv::TableReader::Make(pool, input, read_options, parse_options, convert_options);
		if(reader.ok()){
		std::shared_ptr<arrow::csv::TableReader>tableReader = std::move(reader).ValueOrDie();
		auto t = tableReader->Read();
			if(t.ok()){
				std::shared_ptr<arrow::Table> table = std::move(t).ValueOrDie();
				//int64_t rows = table->num_rows();
				//int64_t columns = table->num_columns();
				//std::cout << "rows:- " << rows << " columns:- " << columns << '\n';
				std::shared_ptr<arrow::ChunkedArray> myfield = table->column(9);
				//std::cout << myfield->length() << std::endl;
				arrow::ArrayVector data = myfield->chunks();
				std::vector<int64_t> vec;
				
				for(int64_t i = 0 ; i < 656 ; i++){
					auto int64_array = std::static_pointer_cast<arrow::Int64Array>(data[i]);
					for(int64_t j = 0; j < data[i]->length() ; j++){
						if(!int64_array->IsNull(j)){
							vec.emplace_back(int64_array->Value(j));
						}
					}	

				}
				
				//std::cout << vec.size() << std::endl;
				std::sort(vec.begin(),vec.end());
				auto [min,max] = std::minmax_element(vec.begin(),vec.end());
				std::cout << "min " << *min << " max " << *max << std::endl;
			}
				
				
		}
	}
}
   	

