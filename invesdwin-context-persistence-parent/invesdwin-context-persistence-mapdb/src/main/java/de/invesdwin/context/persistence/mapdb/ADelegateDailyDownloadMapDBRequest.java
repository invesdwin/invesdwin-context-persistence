package de.invesdwin.context.persistence.mapdb;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.mapdb.DBMaker.Maker;

import de.invesdwin.context.integration.network.request.DailyDownloadCache;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.Threads;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.ProcessedEventsRateString;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class ADelegateDailyDownloadMapDBRequest<K, V> implements IReference<Map<K, V>> {

    private final Log log = new Log(this);

    private final DailyDownloadCache dailyDownloadCache = newDailyDownloadCache();
    private ADelegateMapDB<K, V> map;

    private boolean firstOpen;

    @Override
    public Map<K, V> get() {
        maybeUpdate();
        return map;
    }

    protected void maybeUpdate() {
        if (shouldUpdate()) {
            synchronized (this) {
                try {
                    if (shouldUpdate()) {
                        beforeUpdate();
                        final ICloseableIterator<V> reader = getIterator();
                        final Instant start = new Instant();
                        log.info("Starting indexing [%s] ...", getDownloadFileName());
                        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);
                        try {
                            int count = 0;
                            while (true) {
                                final V value = reader.next();
                                final K key = extractKey(value);
                                map.put(key, value);
                                count++;
                                if (loopCheck.check()) {
                                    printProgress("Processing indexing [" + getDownloadFileName() + "]", start, count);
                                }
                            }
                        } catch (final NoSuchElementException e) {
                            //end reached
                        }
                        afterUpdate();
                        log.info("Finished indexing [%s] after: %s", getDownloadFileName(), start);
                    }
                } catch (final Throwable t) {
                    DailyDownloadCache.delete(getDownloadFileName());
                    map.deleteTable();
                    throw Throwables.propagate(t);
                }
            }
        }
    }

    protected void printProgress(final String action, final Instant start, final int count) {
        final Duration duration = start.toDuration();
        log.info("%s: %s %s during %s", action, count, new ProcessedEventsRateString(count, duration), duration);
    }

    protected boolean shouldUpdate() {
        if (Threads.isInterrupted()) {
            return false;
        }
        try {
            if (map == null) {
                try {
                    map = newMap(false);
                    if (map.isEmpty()) {
                        return true;
                    }
                } finally {
                    map.close();
                    map = newMap(true);
                }
            }
            return map.isEmpty() || dailyDownloadCache.shouldUpdate(getDownloadFileName(), getNow());
        } catch (final Throwable t) {
            Err.process(new RuntimeException("Forcing update due to exception", t));
            return true;
        }
    }

    protected void beforeUpdate() {
        if (!map.isEmpty()) {
            map.deleteTable();
        }
        map.close();
        map = newMap(false);
    }

    protected void afterUpdate() {
        //prevent checksum errors
        map.close();
        map = newMap(true);
    }

    protected abstract K extractKey(V value);

    protected abstract ICloseableIterator<V> newReader(InputStream content);

    protected ADelegateMapDB<K, V> newMap(final boolean readOnly) {
        return new ADelegateMapDB<K, V>(getDownloadName()) {

            @Override
            protected Maker configureDB(final Maker maker) {
                Maker parent = super.configureDB(maker);
                if (readOnly) {
                    parent = parent.readOnly();
                }
                return parent;
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

    public ICloseableIterator<V> getIterator() {
        try {
            final Instant start = new Instant();
            log.info("Starting download [%s] ...", getDownloadFileName());
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
            log.info("Finished download [%s] after: %s", getDownloadFileName(), start);
            return reader;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
