from collections import Counter

import unicodedata
import re
import nltk
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords

stop_words = None
stemmer = None


def tokenizer(text):
    tokens = []
    for word in text.split(" "):
        tokens.append(word)
    return tokens


def remove_stopwords(tokens):
    global stop_words
    if stop_words is None:
        stop_words = set(stopwords.words('english'))
        if not stop_words:
            nltk.download('stopwords')
            stop_words = set(stopwords.words('english'))
    cleaned = []
    for word in tokens:
        if word not in stop_words:
            cleaned.append(word)
    return cleaned


def stemming(tokens):
    global stemmer
    if stemmer is None:
        stemmer = PorterStemmer()
    stems = []
    for word in tokens:
        stems.append(stemmer.stem(word))
    return stems


def clean_text(text):
    # remove punctuation signs (keep only letters and numbers)
    pattern = r'[^a-zA-Z0-9\s]'
    cleaned_text = re.sub(pattern, ' ', text)

    # Replace multiple spaces with a single space
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
    # Strip leading and trailing spaces and convert to lower case
    cleaned_text = cleaned_text.lower().strip()
    return cleaned_text


def preprocess_text(text, skip_stemming=True, allow_stop_words=True):
    clean = clean_text(text)
    tokens = tokenizer(clean)
    if not allow_stop_words:
        tokens = remove_stopwords(tokens)
    if not skip_stemming:
        tokens = stemming(tokens)
    return tokens


def count_token_occurrences(tokens):
    return Counter(tokens)
