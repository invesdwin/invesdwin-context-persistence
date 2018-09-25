package de.invesdwin.context.persistence.timeseries.ezdb;

import java.io.File;
import java.util.Iterator;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.configuration2.AbstractConfiguration;

import de.invesdwin.context.persistence.timeseries.ezdb.ADelegateRangeTable.DelegateTableIterator;
import de.invesdwin.context.system.properties.AProperties;
import de.invesdwin.util.collections.iterable.ATransformingCloseableIterator;
import ezdb.TableRow;

@ThreadSafe
public class RangeTableProperties extends AProperties {

    private final ADelegateRangeTable<Void, String, Object> table;

    public RangeTableProperties(final String name) {
        this.table = newRangeTable(name);
    }

    protected ADelegateRangeTable<Void, String, Object> newRangeTable(final String name) {
        return new ADelegateRangeTable<Void, String, Object>(name) {
            @Override
            protected boolean allowPutWithoutBatch() {
                return true;
            }

            @Override
            protected boolean allowHasNext() {
                return true;
            }

            @Override
            protected File getDirectory() {
                return new File(getBaseDirectory(), RangeTableProperties.class.getSimpleName());
            }
        };
    }

    @Override
    protected AbstractConfiguration createDelegate() {
        return new AbstractConfiguration() {
            @Override
            protected boolean isEmptyInternal() {
                try (DelegateTableIterator<Void, String, Object> range = table.range(null)) {
                    return range.hasNext();
                }
            }

            @Override
            protected Object getPropertyInternal(final String key) {
                return table.get(null, key);
            }

            @Override
            protected Iterator<String> getKeysInternal() {
                final DelegateTableIterator<Void, String, Object> range = table.range(null);
                return new ATransformingCloseableIterator<TableRow<Void, String, Object>, String>(range) {
                    @Override
                    protected String transform(final TableRow<Void, String, Object> value) {
                        return value.getRangeKey();
                    }
                };
            }

            @Override
            protected boolean containsKeyInternal(final String key) {
                return table.get(null, key) != null;
            }

            @Override
            protected void clearPropertyDirect(final String key) {
                table.delete(null, key);
            }

            @Override
            protected void addPropertyDirect(final String key, final Object value) {
                table.put(null, key, value);
            }
        };
    }

}
