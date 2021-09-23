package de.invesdwin.context.persistence.ezdb;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;

import de.invesdwin.context.integration.network.request.DailyDownloadCache;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.ProcessedEventsRateString;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.VoidSerde;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;
import ezdb.batch.Batch;

@NotThreadSafe
public abstract class ADelegateDailyDownloadRangeTableRequest<K, V>
        implements IReference<ADelegateRangeTable<K, Void, V>> {

    private final Log log = new Log(this);

    private final DailyDownloadCache dailyDownloadCache = newDailyDownloadCache();
    private ADelegateRangeTable<K, Void, V> table;

    @Override
    public ADelegateRangeTable<K, Void, V> get() {
        maybeUpdate();
        return table;
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
                        try (Batch<K, V> batch = table.newBatch()) {
                            int count = 0;
                            try {
                                while (true) {
                                    final V value = reader.next();
                                    final K key = extractKey(value);
                                    batch.put(key, value);
                                    count++;
                                    if (count >= ADelegateRangeTable.BATCH_FLUSH_INTERVAL) {
                                        batch.flush();
                                        if (loopCheck.check()) {
                                            printProgress("Processing indexing [" + getDownloadFileName() + "]", start,
                                                    count);
                                        }
                                    }
                                }
                            } catch (final NoSuchElementException e) {
                                //end reached
                            }
                            if (count > 0) {
                                batch.flush();
                            }
                        }
                        afterUpdate();
                        log.info("Finished indexing [%s] after: %s", getDownloadFileName(), start);
                    }
                } catch (final Throwable t) {
                    deleteDownloadedFile();
                    table.deleteTable();
                    throw Throwables.propagate(t);
                }
            }
        }
    }

    protected void deleteDownloadedFile() {
        DailyDownloadCache.delete(getDownloadFileName());
    }

    protected void printProgress(final String action, final Instant start, final int count) {
        final Duration duration = start.toDuration();
        log.info("%s: %s %s during %s", action, count, new ProcessedEventsRateString(count, duration), duration);
    }

    protected boolean shouldUpdate() {
        if (table == null) {
            table = newTable();
        }
        return table.isEmpty() || dailyDownloadCache.shouldUpdate(getDownloadFileName(), getNow());
    }

    protected void beforeUpdate() {
        if (!table.isEmpty()) {
            table.deleteTable();
        }
    }

    protected void afterUpdate() {
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
