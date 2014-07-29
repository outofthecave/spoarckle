from pyspark.accumulators import AccumulatorParam


"""
Select the smallest subset of features that still covers the data.

"Covering" the data means that each instance has at least one non-zero
value in its (non-sparse) vector for the given feature set.  A "vital"
feature is one that cannot be left out without losing data coverage.
A feature set is "maximally reduced" if it contains only vital features.
"""


class QueueAccumulatorParam(AccumulatorParam):
    """
    Generalize Spark's accumulator to queues
    """

    def zero(self, queue):
        return []

    def addInPlace(self, queue1, queue2):
        queue1.extend(queue2)
        return queue1


def getAllMaxReducedFeatSets(all_feats_rdd, data_matrix_rdd, sc):
    """
    Return all maximally reduced feature sets that cover the data

    all_feats_rdd -- RDD of all feature names
    data_matrix_rdd -- RDD of (instance, feature) pairs for those features
        with value 1
    sc -- current SparkContext
    """
    feat_sets = []

    # cache the data matrix, since we use it in the loop below
    data_matrix_rdd.cache()

    # every Spark task can append things to the queue, but only the driver can
    # read it; the order of traversal of the search tree is irrelevant
    queue_acc = sc.accumulator([(None, all_feats_rdd)],
                               accum_param=QueueAccumulatorParam())

    while queue_acc.value:
        prev_feat, feats_rdd = queue_acc.value.pop(0)

        feats_to_1_rdd = feats_rdd.map(lambda feat: (feat, 1))
        # data matrix with only those features that are still 'in the game'
        new_data_matrix_rdd = (data_matrix_rdd
            # RDD = [(inst, feat), ...]
            .map(lambda (inst, feat): (feat, inst))
            # RDD = [(feat, inst), ...]
            .join(feats_to_1_rdd)  # discards features that don't occur in both
            # RDD = [(feat, (inst, 1)), ...]
            .map(lambda (feat, (inst, one)): (inst, feat))
            # RDD = [(inst, feat), ...]
            )
        # remove temporary RDD
        del feats_to_1_rdd

        new_non_vital_feats_rdd = (new_data_matrix_rdd
            # RDD = [(inst, feat), ...]
            .map(lambda (inst, feat): (inst, 1))
            # RDD = [(inst, 1), ...]
            .reduceByKey(lambda count1, count2: count1 + count2)
            # RDD = [(inst, feat_count), ...]
            .join(new_data_matrix_rdd)
            # RDD = [(inst, (feat_count, feat)), ...]
            .map(lambda (inst, (feat_count, feat)): (feat, feat_count == 1))
            # RDD = [(feat, feat_count == 1), ...]
            .reduceByKey(lambda bool1, bool2: bool1 or bool2)
            # RDD = [(feat, is_vital), ...]
            .filter(lambda (feat, boolean): not boolean)
            # RDD = [non_vital_feat, ...]
            )
        del new_data_matrix_rdd

        # if all features are vital, this is a maximally reduced set
        if not new_non_vital_feats_rdd.count():  # forces execution
            feat_sets.append(feats_rdd)
            #return feat_sets  # TODO debugging

        # else, try to furtherly reduce this feature set
        else:
            new_queue_items = (new_non_vital_feats_rdd
                # RDD = [non_vital_feat, ...]
                .cartesian(feats_rdd)
                # RDD = [(non_vital_feat, any_feat), ...]
                .filter(lambda (non_vital_feat, any_feat):
                        # remove a non-vital feature in the next step
                        non_vital_feat != any_feat and
                        # it doesn't matter in which order features are
                        # removed, so don't try removing A after removing B
                        non_vital_feat > prev_feat)
                # RDD = [(non_vital_feat, other_feat), ...]
                .groupByKey()
                # RDD = [(non_vital_feat, feat_list_to_try), ...]
                ).collect()
            while new_queue_items:
                # remove the items from one queue while we're adding them to
                # the other to avoid keeping them around twice (list & RDD)
                (feat, feat_list) = new_queue_items.pop(0)
                feat_list_rdd = sc.parallelize(feat_list)
                queue_acc.add([(feat, feat_list_rdd)])
                del feat, feat_list, feat_list_rdd
        del feats_rdd, new_non_vital_feats_rdd

    return feat_sets


def getSmallestCoveringFeatSets(all_feats_rdd, data_matrix_rdd, sc):
    """
    Return the smallest maximally reduced feature sets that cover the data.
    If there are several such sets of the same size, return them all.

    Parameters are the same as for getAllMaxReducedFeatSets(...)
    """
    smallest_sets = []
    # TODO adjust to new interface
    all_sets = getAllMaxReducedFeatSets(all_feats_rdd, data_matrix_rdd, sc)
    min_size = all_feats_rdd.count()  # forces execution
    # TODO is it worth parallelizing this too?
    for feat_set in all_sets:
        if len(feat_set) < min_size:
            smallest_sets = [feat_set]
        elif len(feat_set) == min_size:
            smallest_sets.append(feat_set)
    return smallest_sets
