from src.config import query_processing_algorithm_config, scoring_function_config, limit_input_rows_config
from src.modules.multiprocessing import open_dataset_multiprocess, add_document_to_index

config = [limit_input_rows_config, query_processing_algorithm_config[1], scoring_function_config[0], 4, True, True, "No"]
if __name__ == "__main__":
    open_dataset_multiprocess(config, add_document_to_index, delete_chunks=False, delete_after_compression=False)
