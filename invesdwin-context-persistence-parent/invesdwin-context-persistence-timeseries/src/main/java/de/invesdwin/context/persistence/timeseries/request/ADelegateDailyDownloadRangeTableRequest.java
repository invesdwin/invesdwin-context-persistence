package de.invesdwin.context.persistence.timeseries.request;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;

import de.invesdwin.context.integration.network.DailyDownloadCache;
import de.invesdwin.context.persistence.timeseries.ezdb.ADelegateRangeTable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.VoidSerde;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public abstract class ADelegateDailyDownloadRangeTableRequest<K, V>
        implements IReference<ADelegateRangeTable<K, Void, V>> {

    private final DailyDownloadCache dailyDownloadCache = newDailyDownloadCache();
    private ADelegateRangeTable<K, Void, V> table;

    @Override
    public ADelegateRangeTable<K, Void, V> get() {
        if (table == null) {
            table = newTable();
        }
        maybeUpdate();
        return table;
    }

    private void maybeUpdate() {
        try {
            final boolean empty = table.isEmpty();
            if (empty || dailyDownloadCache.shouldUpdate(getDownloadFileName(), getNow())) {
                if (!empty) {
                    table.deleteTable();
                }
                final InputStream content = dailyDownloadCache.downloadStream(getDownloadFileName(),
                        new Consumer<OutputStream>() {
                            @Override
                            public void accept(final OutputStream t) {
                                try (InputStream in = download()) {
                                    IOUtils.copy(in, t);
                                } catch (final Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }, getNow());
                final ICloseableIterator<V> reader = newReader(content);
                try {
                    while (true) {
                        final V value = reader.next();
                        final K key = extractKey(value);
                        map.put(key, value);
                    }
                } catch (final NoSuchElementException e) {
                    //end reached
                }
            }
        } catch (final Throwable t) {
            DailyDownloadCache.delete(getDownloadFileName());
            map.deleteTable();
            throw Throwables.propagate(t);
        }
    }

    protected abstract K extractKey(V value);

    protected abstract ICloseableIterator<V> newReader(InputStream content);

    protected ADelegateRangeTable<K, Void, V> newTable() {
        return new ADelegateRangeTable<K, Void, V>(getDownloadName()) {

            @Override
            protected ISerde<Void> newRangeKeySerde() {
                return VoidSerde.GET;
            }

        };
    }

    protected FDate getNow() {
        return new FDate();
    }

    protected DailyDownloadCache newDailyDownloadCache() {
        return new DailyDownloadCache();
    }

    protected String getDownloadFileName() {
        return getDownloadName() + ".lz4";
    }

    protected File getDownloadFile() {
        return DailyDownloadCache.newFile(getDownloadFileName());
    }

    protected abstract String getDownloadName();

    protected abstract InputStream download() throws Exception;

}
