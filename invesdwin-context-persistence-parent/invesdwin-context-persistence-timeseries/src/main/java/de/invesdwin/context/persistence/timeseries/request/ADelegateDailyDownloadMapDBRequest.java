package de.invesdwin.context.persistence.timeseries.request;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;

import de.invesdwin.context.integration.network.DailyDownloadCache;
import de.invesdwin.context.persistence.timeseries.mapdb.ADelegateMapDB;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public abstract class ADelegateDailyDownloadMapDBRequest<K, V> implements IReference<Map<K, V>> {

    private final DailyDownloadCache dailyDownloadCache = newDailyDownloadCache();
    private ADelegateMapDB<K, V> map;

    @Override
    public Map<K, V> get() {
        if (map == null) {
            map = newMap();
        }
        maybeUpdate();
        return map;
    }

    private void maybeUpdate() {
        try {
            if (map.isEmpty() || dailyDownloadCache.shouldUpdate(getDownloadFileName(), getNow())) {
                if (!map.isEmpty()) {
                    map.deleteTable();
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

    protected ADelegateMapDB<K, V> newMap() {
        return new ADelegateMapDB<K, V>(getDownloadName());
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
