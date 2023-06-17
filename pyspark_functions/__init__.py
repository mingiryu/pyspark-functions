from pyspark.sql import functions as F


def array_mean(column):
    name = column._jc.toString()
    column = F.aggregate(
        column, F.lit(0.0), lambda x, y: x + y, lambda acc: acc / F.size(column)
    )
    return column.alias(f"array_mean({name})")


def array_mode(column):
    name = column._jc.toString()
    items = F.array_distinct(column)
    counts = F.transform(
        items,
        lambda x: F.aggregate(
            column, F.lit(0), lambda acc, y: acc + F.when(x == y, 1).otherwise(0)
        ),
    )
    counter = F.map_from_arrays(counts, items)
    column = F.element_at(counter, F.array_max(counts))
    return column.alias(f"array_mode({name})")


def collect_set(column):
    name = column._jc.toString()
    column = F.array_distinct(F.collect_list(column))
    return column.alias(f"collect_set({name})")
