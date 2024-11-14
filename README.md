# MIRCV Project 2023/2024
Group Project for Multimedia Information Retrieval and Computer Vision.

Lorenzo Nelli, Gianluca Antonio Cometa, Andrea Sottile 
# User Guide

## Content
- [How to Use](#how-to-use)
- [Configuration and Files](#configuration-and-files)
- [Parameters](#parameters)
- [Tutorial](#tutorial)

---

## How to Use

Before starting, ensure that the correct paths and collection file are set up. Optionally, pre-existing indexes can be added to explore features without re-indexing from scratch. 

This project includes four executable programs:
1. `main` (user interface)
2. `evaluate_indexes`
3. `create_indexes`
4. `merge_indexes`

Follow the step-by-step instructions in each section to set up and use these features. Begin with configuration setup.

---

## Configuration and Files

The `config.py` file contains essential variables for connecting project files and configuring parameters across modules. This file requires manual updates if transferring the project to a new system.

#### Step-by-Step [part 1]

1. Copy all project files to a location on your PC.
2. Copy the collection file to the appropriate directory.
3. Create a folder for index storage, adding pre-made indexes if needed.
4. Update the `root_directory` path variables in `config.py` (Specify the absolute path of the directory where you downloaded the repository).

### Paths, Input, and Output (All paths should be absolute)

- `root_directory`: Path to root directory of REPO.
- `collection_path_config`: Path to the collection file (compressed or uncompressed).
- `index_config_path`: Path to the folder for index configuration files.
- `index_folder_path`: Path to the folder containing index folders.
- `evaluation_trec_queries_2019_path` and `evaluation_trec_qrel_2019_path`: Paths to TREC 2019 evaluation files.
- `evaluation_trec_queries_2020_path` and `evaluation_trec_qrel_2020_path`: Paths to TREC 2020 evaluation files.
- `output_query_trec_evaluation_file`: Path and name for the TREC evaluation output file.

### Example File Structure

```plaintext
~/
 ├── ROOTDIR/
 │   ├── trec/
 │   │   ├── msmarco-test2020-queries.tsv
 │   │   └── 2020qrels-pass.txt 
 │   ├── evaluation/
 │   │   ├── output.csv
 │   ├── default_index/
 │   │   ├── index.txt
 │   │   ├── lexicon.txt
 │   │   └── stats.txt
 │   ├── my_other_index/
 │   │   ├── index.txt
 │   │   ├── lexicon.txt
 │   │   └── stats.txt
 │   ├── index_info_default_index.txt
 │   ├── index_info_my_other_index.txt
 │   ├── collection.tar.gz
```
### Parameters
#### Step by step [part 2]
- There is no need to change anything in all the other parameters

#### String format config
It is possible to change the separators used to delimit strings, posting lists, fields and lines when indexing a collection. Changing these values would make unusable the indexes made with different configs.
#### Verbosity config
The project makes extensive use of Prints, to keep track of the execution at run-time. This parameter is used to suppress the Prints in a hierarchical way: decreasing the value reduces the number of Prints by ignoring the less important ones. A value of 3 has been used to keep track of timestamps and overall execution in the debug phase. Values between 5-8 slows the program execution and are usable only with breakpoints. 
#### Lists
Some parameters are a list of options. All the options are implemented and working, and it’s possible to pick what to use at runtime. There is nothing to change in these values. 
#### Other configurations
The remaining parameters are relative to the algorithms seen during the lessons, or self-explanatory configurations with no need to be changed.
- `index_chunk_size`
- `limit_input_rows_config`

These two are the most important ones when trying to create a new index. The limit on rows is used to avoid scanning the whole connection for test purposes. The chunk size (measured in characters) determines how many files are created in the indexing phase, and they’re going to be merged after scanning the collection.



### Tutorial

There are 4 programs that can be run independently in this project.

#### Step by step [part 3]
- Launch any one execution of these files
- Wait for it to terminate before using it again or starting the other ones.


#### Creating Indexes (`create_indexes`)
Launch the execution of N processes (the parameter “parts” of the first function) to create N indexes. Each one contains 1/N of the input collection, made with consecutive rows; therefore, the parts are mutually exclusive. Splitting the collection might lose up to N documents since the cuts happen by counting the number of characters and ignoring the rows, and the half-rows are skipped. Each index is independent from the others, and this execution is designed to be run on multi-core machines to speed up the indexing phase. Each index is named with a pattern that hints at the part of the collection it contains. This execution takes a lot of time and CPU resources.

#### Merging Indexes (`merge_indexes`)
This is made to merge the product of create_indexes. A list of indexes are ordered and merged to produce one output index, relying on document names and the token’s alphabetical order. In addition, all the document names are checked to be present: for each missing document, the collection is opened again to create one more index; in this way, the merge phase starts only when all the documents are present in one and only one index. Optionally, this phase can apply a compression algorithm. This execution takes a lot of time and CPU resources, and it’s designed to run on a single core.
The indexes to merge must be manually configured.

#### Evaluating Indexes (`evaluate_indexes`)
A parameter is required to be manually selected to choose between the two Trec sets provided. The program instantiates a catalog of indexes (expecting them to be present), and runs a trec evaluation on each one. Using multiple indexes during one evaluation is useful to make different configurations of parameters, but the result is the same by using only one and running the evaluation multiple times. The output is saved in a file.


### Main: User Interface
This is the only program where the user can interact with at runtime. A list of choices is presented at each input, and the user can navigate them with the keyboard inputs. Most of the choices just require inputting the number associated, but it’s also possible to edit the parameter configurations for indexing and queries by typing. The program loops ignoring unacceptable requests, representing the question or going back to the main menu.
It is important to notice that some choices affect the index on disk, and not only the query at runtime. There are some options marked with a “Restart Required” flag. The flag is raised when some parameter is inconsistent with the index currently loaded. The flag can be cleared by reverting the parameters, reloading the index from disk, or reindexing the collection. The reindexing option has been disabled to avoid mistakes. A default index is loaded when the program is started. If there is no default index, one is created with default parameters.
At each user input, it’s required to press the Enter key to start the command execution.

#### Step by step [part 4]
##### Creating a new index from the main interface:
- Start the program
- Use the option 1 to change the parameters (in the nested menu)
- Save the index parameters on disk creating a new index_info file (optional)
- Executing any query (even a blank one) starts the indexing

##### Loading a pre-made index:
- Start the program
- Use the option 1 to change the index title (option 8 in the nested menu)
- Load the parameters (the title must be changed to match the index on hdd)
- The reload flag is cleared after the loading
##### Running a query
- Start the program and load an index (the default index is ok)
- Use the option 4 to enter the query text
- Use the option 5 to run the query



