# CCF-Dedup

from pyspark import RDD


def dedup(pairs: RDD) -> RDD:
    #return pairs.distinct()
    # Implementation of the paper:
    return pairs.map(lambda kv: (kv, None))\
        .reduceByKey(lambda a,_: a)\
        .map(lambda kv: kv[0])
    
