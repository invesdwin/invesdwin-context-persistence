package de.invesdwin.context.persistence.ezdb.table;

import java.util.function.Function;
import java.util.function.Supplier;

import ezdb.table.Table;

public interface IDelegateTable<H, V> extends Table<H, V> {

    void deleteTable();

    V getOrLoad(H hashKey, Function<? super H, ? extends V> loadable);

    V getOrLoad(H hashKey, Supplier<V> loadable);

}
