import unicodedata
import re
import nltk
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords

stop_words = None
stemmer = None

def preprocess_init(self, filepath, use_stemming=False, remove_stopwords=False):
    stop_words = set(stopwords.words('english'))
    if not stop_words:
        nltk.download('stopwords')
        stop_words = set(stopwords.words('english'))

    self.filepath = filepath
    self.use_stemming = use_stemming
    self.remove_stopwords = remove_stopwords
    stemmer = PorterStemmer()
    print(stop_words)

    # convert to ascii


#  text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')

def remove_stopwords(text):
    global stop_words
    if stop_words is None:
        stop_words = set(stopwords.words('english'))
        if not stop_words:
            nltk.download('stopwords')
            stop_words = set(stopwords.words('english'))
    cleaned = []
    for word in text.split(" "):
        if word not in stop_words:
            cleaned.append(word)
    return " ".join(cleaned)


def stemming(text):
    global stemmer
    if stemmer is None:
        stemmer = PorterStemmer()
    stems = []
    for word in text.split(" "):
        stems.append(stemmer.stem(word))
    return " ".join(stems)


def clean_text(text):
    # remove punctuation signs (keep only letters and numbers)
    pattern = r'[^a-zA-Z0-9\s]'
    cleaned_text = re.sub(pattern, ' ', text)

    # Replace multiple spaces with a single space
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
    # Strip leading and trailing spaces and convert to lower case
    cleaned_text = cleaned_text.lower().strip()
    return cleaned_text
