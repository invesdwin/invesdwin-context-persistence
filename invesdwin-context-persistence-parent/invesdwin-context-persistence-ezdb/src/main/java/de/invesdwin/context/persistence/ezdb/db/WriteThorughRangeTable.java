package de.invesdwin.context.persistence.ezdb.db;

import java.io.IOException;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import ezdb.table.Batch;
import ezdb.table.RangeTableRow;
import ezdb.table.range.RangeBatch;
import ezdb.table.range.RangeTable;
import ezdb.util.TableIterator;

@NotThreadSafe
public class WriteThorughRangeTable<H, R, V> implements RangeTable<H, R, V> {

    private final RangeTable<H, R, V> memory;
    private final RangeTable<H, R, V> disk;
    private final ALoadingCache<H, RangeTable<H, R, V>> hashKey_loadedMemory = new ALoadingCache<H, RangeTable<H, R, V>>() {
        @Override
        protected RangeTable<H, R, V> loadValue(final H key) {
            loadHashKeyIntoMemory(key);
            return memory;
        }

        @Override
        protected boolean isHighConcurrency() {
            return true;
        }
    };

    public WriteThorughRangeTable(final RangeTable<H, R, V> memory, final RangeTable<H, R, V> disk) {
        this.memory = memory;
        this.disk = disk;
    }

    private void loadHashKeyIntoMemory(final H hashKey) {
        final TableIterator<RangeTableRow<H, R, V>> range = disk.range(hashKey);
        final RangeBatch<H, R, V> batch = memory.newRangeBatch();
        try {
            int count = 0;
            try {
                while (true) {
                    final RangeTableRow<H, R, V> next = range.next();
                    batch.put(next.getHashKey(), next.getRangeKey(), next.getValue());
                    count++;
                    if (count >= ADelegateRangeTable.BATCH_FLUSH_INTERVAL) {
                        count = 0;
                    }
                }
            } catch (final NoSuchElementException e) {
                //end reached
            }
            if (count > 0) {
                batch.flush();
            }
        } finally {
            range.close();
            try {
                batch.close();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void put(final H hashKey, final V value) {
        final RangeTable<H, R, V> loadedMemory = hashKey_loadedMemory.getIfPresent(hashKey);
        if (loadedMemory != null) {
            loadedMemory.put(hashKey, value);
        }
        disk.put(hashKey, value);
    }

    @Override
    public V get(final H hashKey) {
        return hashKey_loadedMemory.get(hashKey).get(hashKey);
    }

    @Override
    public void delete(final H hashKey) {
        memory.delete(hashKey);
        disk.delete(hashKey);
    }

    @Override
    public void close() {
        disk.close();
        memory.close();
        hashKey_loadedMemory.clear();
    }

    @Override
    public Batch<H, V> newBatch() {
        return newRangeBatch();
    }

    @Override
    public void put(final H hashKey, final R rangeKey, final V value) {
        final RangeTable<H, R, V> loadedMemory = hashKey_loadedMemory.getIfPresent(hashKey);
        if (loadedMemory != null) {
            loadedMemory.put(hashKey, rangeKey, value);
        }
        disk.put(hashKey, rangeKey, value);
    }

    @Override
    public V get(final H hashKey, final R rangeKey) {
        return hashKey_loadedMemory.get(hashKey).get(hashKey, rangeKey);
    }

    @Override
    public RangeTableRow<H, R, V> getLatest(final H hashKey) {
        return hashKey_loadedMemory.get(hashKey).getLatest(hashKey);
    }

    @Override
    public RangeTableRow<H, R, V> getLatest(final H hashKey, final R rangeKey) {
        return hashKey_loadedMemory.get(hashKey).getLatest(hashKey, rangeKey);
    }

    @Override
    public RangeTableRow<H, R, V> getNext(final H hashKey, final R rangeKey) {
        return hashKey_loadedMemory.get(hashKey).getNext(hashKey, rangeKey);
    }

    @Override
    public RangeTableRow<H, R, V> getPrev(final H hashKey, final R rangeKey) {
        return hashKey_loadedMemory.get(hashKey).getPrev(hashKey, rangeKey);
    }

    @Override
    public TableIterator<RangeTableRow<H, R, V>> range(final H hashKey) {
        return hashKey_loadedMemory.get(hashKey).range(hashKey);
    }

    @Override
    public TableIterator<RangeTableRow<H, R, V>> range(final H hashKey, final R fromRangeKey) {
        return hashKey_loadedMemory.get(hashKey).range(hashKey, fromRangeKey);
    }

    @Override
    public TableIterator<RangeTableRow<H, R, V>> range(final H hashKey, final R fromRangeKey, final R toRangeKey) {
        return hashKey_loadedMemory.get(hashKey).range(hashKey, fromRangeKey, toRangeKey);
    }

    @Override
    public TableIterator<RangeTableRow<H, R, V>> rangeReverse(final H hashKey) {
        return hashKey_loadedMemory.get(hashKey).rangeReverse(hashKey);
    }

    @Override
    public TableIterator<RangeTableRow<H, R, V>> rangeReverse(final H hashKey, final R fromRangeKey) {
        return hashKey_loadedMemory.get(hashKey).rangeReverse(hashKey, fromRangeKey);
    }

    @Override
    public TableIterator<RangeTableRow<H, R, V>> rangeReverse(final H hashKey, final R fromRangeKey,
            final R toRangeKey) {
        return hashKey_loadedMemory.get(hashKey).rangeReverse(hashKey, fromRangeKey, toRangeKey);
    }

    @Override
    public void delete(final H hashKey, final R rangeKey) {
        memory.delete(hashKey, rangeKey);
        disk.delete(hashKey, rangeKey);
    }

    @Override
    public void deleteRange(final H hashKey) {
        memory.deleteRange(hashKey);
        disk.deleteRange(hashKey);
    }

    @Override
    public void deleteRange(final H hashKey, final R fromRangeKey) {
        memory.deleteRange(hashKey, fromRangeKey);
        disk.deleteRange(hashKey, fromRangeKey);
    }

    @Override
    public void deleteRange(final H hashKey, final R fromRangeKey, final R toRangeKey) {
        memory.deleteRange(hashKey, fromRangeKey, toRangeKey);
        disk.deleteRange(hashKey, fromRangeKey, toRangeKey);
    }

    @Override
    public RangeBatch<H, R, V> newRangeBatch() {
        return new RangeBatch<H, R, V>() {

            private final RangeBatch<H, R, V> memoryBatch = memory.newRangeBatch();
            private final RangeBatch<H, R, V> diskBatch = disk.newRangeBatch();

            @Override
            public void put(final H hashKey, final V value) {
                if (isHashKeyLoaded(hashKey)) {
                    memoryBatch.put(hashKey, value);
                }
                diskBatch.put(hashKey, value);
            }

            @Override
            public void delete(final H hashKey) {
                memoryBatch.delete(hashKey);
                diskBatch.delete(hashKey);
            }

            @Override
            public void flush() {
                memoryBatch.flush();
                diskBatch.flush();
            }

            @Override
            public void close() throws IOException {
                memoryBatch.close();
                diskBatch.close();
            }

            @Override
            public void put(final H hashKey, final R rangeKey, final V value) {
                if (isHashKeyLoaded(hashKey)) {
                    memoryBatch.put(hashKey, rangeKey, value);
                }
                diskBatch.put(hashKey, rangeKey, value);
            }

            @Override
            public void delete(final H hashKey, final R rangeKey) {
                memoryBatch.delete(hashKey, rangeKey);
                diskBatch.delete(hashKey, rangeKey);
            }

            private boolean isHashKeyLoaded(final H hashKey) {
                return hashKey_loadedMemory.containsKey(hashKey);
            }
        };
    }

}
