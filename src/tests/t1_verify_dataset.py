from src.modules.documentProcessing import open_dataset

data = open_dataset()

# data is a dataframe. it contains two columns: index and value.
# each value is like this : id \t text
# index and id are supposed to match, for each row

print(data)
#print(data.dtype)
#print(len(data))
