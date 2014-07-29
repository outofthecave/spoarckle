import os
import sys
from pyspark import SparkContext, SparkConf

from spoarckle import feature_extractor, feature_selector, sporcle


if __name__ == "__main__":
    appName = "Spoarckle"
    master = "local"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)

    cwd = os.path.dirname(sys.argv[0])
    data_rel_path = os.path.join("..", "..", "resources",
        "Canadian_cities.txt")
    data_abs_path = os.path.abspath(os.path.join(cwd, data_rel_path))
    data_rdd = sc.textFile(data_abs_path)

    ngrams_rdd, data_matrix_rdd = feature_extractor.extract(data_rdd, n=3,
        tokenize=sporcle.tokenize)

    best_feat_sets = feature_selector.getAllMaxReducedFeatSets(ngrams_rdd,
        data_matrix_rdd, sc)
    #for feat_set in best_feat_sets:  # TODO debugging
    #    print feat_set.collect()

    sc.stop()
