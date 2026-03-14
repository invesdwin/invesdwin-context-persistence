package de.invesdwin.context.persistence.ezdb;

import java.io.File;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.configuration2.AbstractConfiguration;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable.DelegateRangeTableIterator;
import de.invesdwin.context.system.properties.AProperties;
import de.invesdwin.util.collections.iterable.ATransformingIterator;
import ezdb.table.RangeTableRow;

@ThreadSafe
public class RangeTableProperties extends AProperties {

    private final ADelegateRangeTable<Void, String, Object> table;

    public RangeTableProperties(final String name) {
        this.table = newRangeTable(name);
    }

    protected ADelegateRangeTable<Void, String, Object> newRangeTable(final String name) {
        return new ADelegateRangeTable<Void, String, Object>(name) {
            @Override
            protected boolean allowHasNext() {
                return true;
            }

            @Override
            protected File getDirectory() {
                return new File(RangeTableProperties.this.getBaseDirectory(),
                        RangeTableProperties.class.getSimpleName());
            }
        };
    }

    protected File getBaseDirectory() {
        return ContextProperties.getHomeDataDirectory();
    }

    @Override
    protected AbstractConfiguration createDelegate() {
        return new AbstractConfiguration() {
            @Override
            protected boolean isEmptyInternal() {
                try (DelegateRangeTableIterator<Void, String, Object> range = table.range(null)) {
                    return range.hasNext();
                }
            }

            @Override
            protected Object getPropertyInternal(final String key) {
                return table.get(null, key);
            }

            @Override
            protected Iterator<String> getKeysInternal() {
                final DelegateRangeTableIterator<Void, String, Object> range = table.range(null);
                return new ATransformingIterator<RangeTableRow<Void, String, Object>, String>(range) {
                    @Override
                    protected String transform(final RangeTableRow<Void, String, Object> value) {
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

            @Override
            protected boolean containsValueInternal(final Object value) {
                final DelegateRangeTableIterator<Void, String, Object> range = table.range(null);
                try {
                    while (true) {
                        final RangeTableRow<Void, String, Object> row = range.next();
                        if (value == row.getValue()) {
                            return true;
                        }
                    }
                } catch (final NoSuchElementException e) {
                    //end reached
                }
                return false;
            }
        };
    }

}
