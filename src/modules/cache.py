from src.config import lexicon_cache_size
from src.modules.utils import print_log

lexicon_cache = {}
cache_count = {}


def cache_check(target_path):
    # check if cache already contains target file path. initialize the structure if not ready yet.
    global lexicon_cache, cache_count
    if target_path in lexicon_cache.keys():
        return  # check ok
    else:
        # initialization
        lexicon_cache[target_path] = []
        cache_count[target_path] = 0


def cache_flush():
    # remove all the content of the cash, but keep the inizialization (path of files)
    global lexicon_cache, cache_count
    for source in lexicon_cache.keys():
        lexicon_cache[source] = []
        cache_count[source] = 0


def cache_hit_or_miss(target_path, target_key):
    global lexicon_cache
    # check if the element is cached or not (returns -1 or position in cache)
    cache_check(target_path)
    if not lexicon_cache[target_path]:
        #if I enter here it means that lexicon_cache[target_path] is empty so we have to insert the first cache entry
        print_log(f"cache miss for {target_key} - first cache entry", 2)
        return -1  # miss: cache empty
    else:
        # remember: cache is not in order
        for line_dict in lexicon_cache[target_path]:
            # each cache line is a dict {"key": "token freq", "posting list")
            if target_key == line_dict["token_key"]:
                print_log(f"cache hit for {target_key}", 2)
                return lexicon_cache[target_path].index(line_dict)  # hit (return cache position)
    print_log(f"cache miss for {target_key}", 2)
    return -1  # miss: not found


def cache_get_token_freq(target_path, cache_position):
    global lexicon_cache
    # fetch an element from the cache
    cache_check(target_path)

    value_dict = lexicon_cache[target_path][cache_position]

    # retaining policy: most recent document is on top
    lexicon_cache[target_path].remove(value_dict)
    lexicon_cache[target_path].append(value_dict)
    return value_dict["token_docfreq"]


def cache_get_posting_list(target_path, cache_position):
    global lexicon_cache
    # fetch an element from the cache
    cache_check(target_path)

    value_dict = lexicon_cache[target_path][cache_position]

    # retaining policy: most recent document is on top
    lexicon_cache[target_path].remove(value_dict)
    lexicon_cache[target_path].append(value_dict)
    return value_dict["token_posting"]


def cache_pop(target_path):
    global lexicon_cache
    # removes an element from the cache (returns the dict)
    cache_check(target_path)
    if not lexicon_cache[target_path]:
        return None  # nothing to pop
    else:
        # retention: pop the oldest element
        oldest_element = lexicon_cache[target_path][0]
        lexicon_cache[target_path].remove(oldest_element)
        return oldest_element


def cache_push(target_path, target_key, target_docfreq, target_posting_list):
    global lexicon_cache, cache_count
    # adds an element to the cache
    # WARNING: this function is called only in the case that there is a miss
    # so we are sure that we have to only add the new element
    new_element = {"token_key": target_key, "token_docfreq": target_docfreq, "token_posting": target_posting_list}
    lexicon_cache[target_path].append(new_element)

    if cache_count[target_path] > lexicon_cache_size:
        # cache full
        cache_pop(target_path)
