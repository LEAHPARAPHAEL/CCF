from pyspark import Accumulator, RDD
# Notre idée de faire des min intelligents (qui est mauvaise au final :/).


# (c'est un max_int la, en pratique faudrait chercher un seuil mais vu que c'est notre méthode c'estg pas nécessaire.

BROADCAST_THRESHOLD = 1_000_000_000


def iterate(pairs: RDD, new_pair_acc: Accumulator) -> RDD:
    sc = pairs.context

    symmetric = pairs.flatMap(lambda kv: [kv, (kv[1], kv[0])])
    symmetric.cache()

    min_rdd = symmetric.reduceByKey(min)
    propagate = min_rdd.filter(lambda kv: kv[1] < kv[0])
    propagate.cache() # le cache c'est la même astuce que dans la v1 (c.f. là-bas pour le commentaire)
    # oui c'était aussi long à écrire que de juste réécrire le commentaire mais pg.

    if propagate.count() <= BROADCAST_THRESHOLD:
        min_dict_broadcast = sc.broadcast(propagate.collectAsMap())

        def emit_with_broadcast(kv):
            node, neighbour = kv
            min_dict = min_dict_broadcast.value
            if node in min_dict:
                min_val = min_dict[node]
                if neighbour != min_val:
                    new_pair_acc.add(1)
                    return [(neighbour, min_val)]
            return []

        new_edges = symmetric.flatMap(emit_with_broadcast)
    else:
        # On broadcast pas si c'est trop grand car sinon ce serait pas rentable.
        # En pratique j'ai mis une valeur super grande car j'avais pas le temps de chercher un valeur optimale.
        # Donc faut pas en parler dans le rapport,
        # mais je l'ai codé donc je le laisse.
        joined = propagate.join(symmetric)

        def emit_neighbours(kv):
            _, (min_val, neighbour) = kv
            if neighbour != min_val:
                new_pair_acc.add(1)
                return [(neighbour, min_val)]
            return []

        new_edges = joined.flatMap(emit_neighbours)

    result = propagate.union(new_edges)
    symmetric.unpersist()
    propagate.unpersist()
    return result
