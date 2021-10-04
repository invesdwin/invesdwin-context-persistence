package de.invesdwin.context.persistence.ezdb.table.range;

import de.invesdwin.context.persistence.ezdb.table.IDelegateTable;
import ezdb.table.range.RangeTable;

public interface IDelegateRangeTable<H, R, V> extends IDelegateTable<H, V>, RangeTable<H, R, V> {

}
