package de.invesdwin.context.persistence.ezdb.table;

import ezdb.table.Table;

public interface IDelegateTable<H, V> extends Table<H, V> {

    void deleteTable();

}
