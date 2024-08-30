import time

from src.modules.documentProcessing import open_dataset, fetch_data_row_from_collection

print("reading some files")
data = open_dataset(count_limit=300)

# data is a dataframe. it contains two columns: index and value.
# each value is like this : id \t text
# index and id are supposed to match, for each row

print(data)


def test_read(docid):
    tic = time.perf_counter()
    d, t = fetch_data_row_from_collection(docid)
    print(d)
    print(t[0])
    toc = time.perf_counter()
    print(f"read completed in {toc - tic:0.4f} seconds")


# docid    # query on compressed   # query on uncompressed
test_read(16)  # 10.57 s                0.0036 s
test_read(726)  # 10.83 s                0.0030 s
test_read(1032456)  # 13.18 s                2.6074 s
test_read(7833456)  # 29.48 s                19.9869 s
test_read(65562)  # 11,24 s                0.1576 s
test_read(684564)  # 12,39 s                1.7179 s
# the time is different thanks to panda's skiprows+nrows.
# without using skiprows+nrows, each teat_read takes 49-52 sec
