from pyspark import Accumulator, RDD


def iterate(pairs: RDD, new_pair_acc: Accumulator) -> RDD:
    # C'est la version naïve du papier.
    symmetric = pairs.flatMap(lambda kv: [kv, (kv[1], kv[0])])
    def reduce_group(kv):
        key, values = kv
        value_list = list(values)
        min_val = min(value_list) # L'opération min elle est catastrophique mais en même temps on a déjà payé la matérialisation.
        if min_val >= key:
            return []

        out = [(key, min_val)]
        for value in value_list:
            if value != min_val:
                new_pair_acc.add(1)
                out.append((value, min_val))
        return out

    return symmetric.groupByKey().flatMap(reduce_group)
