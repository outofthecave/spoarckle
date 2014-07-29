from pyspark import SparkContext, SparkConf


def extract(data_rdd, n=3, tokenize=lambda x: [x]):
    """
    Extract character n-grams as features from words. Return the set of all
    feature names and a set of (word, n-gram) tuples for those n-gram
    features with value 1.

    data_rdd -- RDD of words
    n -- the length of each n-gram (default: 3)
    tokenize -- callable that splits each data item into tokens. This
        doesn't have to be a tokenizer in the strict sense. It may also
        e.g., expand 'st' to 'saint'. It defaults to a dummy tokenizer
        that returns the entire string as one token.
    """
    def _extract_substrings(word, n, tokenize):
        """ Return the set of all char n-grams in this word """
        ngrams = set()
        for token in tokenize(word):
            for i in range(len(token) - n + 1):
                ngrams.add((word, token[i:i + n]))
        return ngrams

    data_matrix_rdd = data_rdd.flatMap(lambda w: _extract_substrings(w, n, tokenize))
    ngrams_rdd = data_matrix_rdd.map(lambda pair: pair[1]).distinct()
    return ngrams_rdd, data_matrix_rdd


if __name__ == "__main__":
    appName = "Spoarckle feature extractor"
    master = "local"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)

    data = ["ottawa", "oshawa"]
    data_rdd = sc.parallelize(data)
    ngrams_rdd, data_matrix_rdd = extract(data_rdd)
    print ngrams_rdd.collect()
    print data_matrix_rdd.collect()

    sc.stop()
