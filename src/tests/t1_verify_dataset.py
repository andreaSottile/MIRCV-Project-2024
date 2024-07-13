from src.modules.documentProcessing import fetch_data_row_from_collection

data = fetch_data_row_from_collection(4)

# data is a dataframe. it contains two columns: index and value.
# each value is like this : id \t text
# index and id are supposed to match, for each row

print(data)
print(data.dtype)
print(len(data))
