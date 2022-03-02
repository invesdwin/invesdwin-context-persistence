package de.invesdwin.context.persistence.ezdb.table.range;

import java.util.function.Function;
import java.util.function.Supplier;

import de.invesdwin.context.persistence.ezdb.table.IDelegateTable;
import de.invesdwin.util.bean.tuple.Pair;
import ezdb.table.range.RangeTable;

public interface IDelegateRangeTable<H, R, V> extends IDelegateTable<H, V>, RangeTable<H, R, V> {

    V getOrLoad(H hashKey, R rangeKey, Function<Pair<H, R>, V> loadable);

    V getOrLoad(H hashKey, R rangeKey, Supplier<V> loadable);

}
