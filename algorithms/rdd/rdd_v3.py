from pyspark import Accumulator, RDD
# La version secondary sort du papier.

def build_secondary_sorted_rdd(pairs: RDD) -> RDD:
    n_parts = pairs.getNumPartitions()
    keyed = pairs.flatMap(lambda kv: [(kv, None), ((kv[1], kv[0]), None)])
    return keyed.repartitionAndSortWithinPartitions(
        numPartitions=n_parts,
        partitionFunc=lambda kv: hash(kv[0]) % n_parts,
    )


def emit_from_secondary_sorted_rdd(sorted_rdd: RDD, new_pair_acc: Accumulator) -> RDD:
    def process_partition(iterator):
        current_key = None
        min_val = None
        should_emit = False

        for (key, value), _ in iterator:
            if key != current_key:
                current_key = key
                min_val = value
                should_emit = min_val < key
                if should_emit:
                    yield (key, min_val)
            elif should_emit:
                new_pair_acc.add(1)
                yield (value, min_val)

    return sorted_rdd.mapPartitions(process_partition)


def iterate(pairs: RDD, new_pair_acc: Accumulator) -> RDD:
    return emit_from_secondary_sorted_rdd(build_secondary_sorted_rdd(pairs), new_pair_acc)
