package de.invesdwin.context.persistence.timeseriesdb.storage;

import java.io.File;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.persistentmap.APersistentMap;
import de.invesdwin.context.integration.persistentmap.CorruptedStorageException;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.context.persistence.ezdb.RangeTablePersistenceMode;
import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseriesdb.IPersistentMapType;
import de.invesdwin.context.persistence.timeseriesdb.PersistentMapType;
import de.invesdwin.context.persistence.timeseriesdb.storage.key.HashRangeKey;
import de.invesdwin.context.persistence.timeseriesdb.storage.key.HashRangeKeySerde;
import de.invesdwin.context.persistence.timeseriesdb.storage.key.HashRangeShiftUnitsKey;
import de.invesdwin.context.persistence.timeseriesdb.storage.key.HashRangeShiftUnitsKeySerde;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.LongSerde;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class TimeSeriesStorage {

    /**
     * LevelDB should be smaller on disk than ChronicleMap and supports range removals during updates. Though LevelDB is
     * a lot slower than ChronicleMap.
     */
    public static final PersistentMapType DEFAULT_MAP_TYPE = PersistentMapType.DISK_FAST;
    private final File directory;
    private final ICompressionFactory compressionFactory;
    private final ADelegateRangeTable<String, FDate, MemoryFileSummary> fileLookupTable;
    private final APersistentMap<HashRangeKey, Long> latestValueIndexLookupTable;
    private final APersistentMap<HashRangeShiftUnitsKey, Long> previousValueIndexLookupTable;
    private final APersistentMap<HashRangeShiftUnitsKey, Long> nextValueIndexLookupTable;

    public TimeSeriesStorage(final File directory, final Integer valueFixedLength,
            final ICompressionFactory compressionFactory) {
        this.directory = directory;
        this.compressionFactory = compressionFactory;
        this.fileLookupTable = new ADelegateRangeTable<String, FDate, MemoryFileSummary>("fileLookupTable") {

            @Override
            protected boolean allowHasNext() {
                return true;
            }

            @Override
            protected File getDirectory() {
                return directory;
            }

            @Override
            protected void onDeleteTableFinished() {
                throw new CorruptedStorageException(getName());
            }

            @Override
            protected ISerde<MemoryFileSummary> newValueSerde() {
                return new MemoryFileSummarySerde(valueFixedLength);
            }

            @Override
            protected RangeTablePersistenceMode getPersistenceMode() {
                return RangeTablePersistenceMode.MEMORY_WRITE_THROUGH_DISK;
            }

        };
        this.latestValueIndexLookupTable = new APersistentMap<HashRangeKey, Long>("latestValueLookupTable") {

            @Override
            public File getDirectory() {
                return directory;
            }

            @Override
            public ISerde<Long> newValueSerde() {
                return LongSerde.GET;
            }

            @Override
            public ISerde<HashRangeKey> newKeySerde() {
                return HashRangeKeySerde.GET;
            }

            //            @Override
            //            protected void onDeleteTableFinished() {
            //                throw new CorruptedTimeSeriesStorageException(getName());
            //            }

            @Override
            protected IPersistentMapFactory<HashRangeKey, Long> newFactory() {
                return getMapType().newFactory();
            }

        };
        this.nextValueIndexLookupTable = new APersistentMap<HashRangeShiftUnitsKey, Long>("nextValueLookupTable") {

            @Override
            public File getDirectory() {
                return directory;
            }

            @Override
            public ISerde<HashRangeShiftUnitsKey> newKeySerde() {
                return HashRangeShiftUnitsKeySerde.GET;
            }

            @Override
            public ISerde<Long> newValueSerde() {
                return LongSerde.GET;
            }

            //            @Override
            //            protected void onDeleteTableFinished() {
            //                throw new CorruptedTimeSeriesStorageException(getName());
            //            }

            @Override
            protected IPersistentMapFactory<HashRangeShiftUnitsKey, Long> newFactory() {
                return getMapType().newFactory();
            }
        };
        this.previousValueIndexLookupTable = new APersistentMap<HashRangeShiftUnitsKey, Long>(
                "previousValueLookupTable") {

            @Override
            public File getDirectory() {
                return directory;
            }

            @Override
            public ISerde<HashRangeShiftUnitsKey> newKeySerde() {
                return HashRangeShiftUnitsKeySerde.GET;
            }

            @Override
            public ISerde<Long> newValueSerde() {
                return LongSerde.GET;
            }

            //            @Override
            //            protected void onDeleteTableFinished() {
            //                throw new CorruptedTimeSeriesStorageException(getName());
            //            }

            @Override
            protected IPersistentMapFactory<HashRangeShiftUnitsKey, Long> newFactory() {
                return getMapType().newFactory();
            }
        };
    }

    /**
     * Can override this to use e.g. DISK_FAST instead
     */
    protected IPersistentMapType getMapType() {
        return DEFAULT_MAP_TYPE;
    }

    public File getDirectory() {
        return directory;
    }

    public ICompressionFactory getCompressionFactory() {
        return compressionFactory;
    }

    public ADelegateRangeTable<String, FDate, MemoryFileSummary> getFileLookupTable() {
        return fileLookupTable;
    }

    public void close() {
        fileLookupTable.close();
        latestValueIndexLookupTable.close();
        previousValueIndexLookupTable.close();
        nextValueIndexLookupTable.close();
    }

    public File newDataDirectory(final String hashKey) {
        return new File(getDirectory(), "storage/" + hashKey);
    }

    public void deleteRange_latestValueIndexLookupTable(final String hashKey) {
        if (getMapType().isRemoveFullySupported()) {
            latestValueIndexLookupTable.removeAll((key) -> {
                return hashKey.equals(key.getHashKey());
            });
        } else {
            //chronicle map does not really support deleting entries, the file size gets bloaded which causes significant I/O
            if (!latestValueIndexLookupTable.isEmpty()) {
                latestValueIndexLookupTable.deleteTable();
            }
        }
    }

    public void deleteRange_latestValueIndexLookupTable(final String hashKey, final FDate above) {
        if (above == null) {
            deleteRange_latestValueIndexLookupTable(hashKey);
        } else {
            if (getMapType().isRemoveFullySupported()) {
                latestValueIndexLookupTable.removeAll((key) -> {
                    return hashKey.equals(key.getHashKey()) && key.getRangeKey().isAfterOrEqualToNotNullSafe(above);
                });
            } else {
                //chronicle map does not really support deleting entries, the file size gets bloaded which causes significant I/O
                if (!latestValueIndexLookupTable.isEmpty()) {
                    latestValueIndexLookupTable.deleteTable();
                }
            }
        }
    }

    public void deleteRange_nextValueIndexLookupTable(final String hashKey) {
        if (getMapType().isRemoveFullySupported()) {
            nextValueIndexLookupTable.removeAll((key) -> {
                return hashKey.equals(key.getHashKey());
            });
        } else {
            //chronicle map does not really support deleting entries, the file size gets bloaded which causes significant I/O
            if (!latestValueIndexLookupTable.isEmpty()) {
                nextValueIndexLookupTable.deleteTable();
            }
        }
    }

    public void deleteRange_previousValueIndexLookupTable(final String hashKey) {
        if (getMapType().isRemoveFullySupported()) {
            previousValueIndexLookupTable.removeAll((key) -> {
                return hashKey.equals(key.getHashKey());
            });
        } else {
            //chronicle map does not really support deleting entries, the file size gets bloaded which causes significant I/O
            if (!previousValueIndexLookupTable.isEmpty()) {
                previousValueIndexLookupTable.deleteTable();
            }
        }
    }

    public void deleteRange_previousValueIndexLookupTable(final String hashKey, final FDate above) {
        if (above == null) {
            deleteRange_previousValueIndexLookupTable(hashKey);
        } else {
            if (getMapType().isRemoveFullySupported()) {
                previousValueIndexLookupTable.removeAll((key) -> {
                    return hashKey.equals(key.getHashKey()) && key.getRangeKey().isAfterOrEqualToNotNullSafe(above);
                });
            } else {
                //chronicle map does not really support deleting entries, the file size gets bloaded which causes significant I/O
                if (!previousValueIndexLookupTable.isEmpty()) {
                    previousValueIndexLookupTable.deleteTable();
                }
            }
        }
    }

    public long getOrLoad_latestValueIndexLookupTable(final String hashKey, final FDate key,
            final Supplier<Long> loadable) {
        final Long index = latestValueIndexLookupTable.getOrLoad(new HashRangeKey(hashKey, key), loadable);
        if (index == null) {
            return -1L;
        } else {
            return index;
        }
    }

    public long getOrLoad_nextValueIndexLookupTable(final String hashKey, final FDate date, final int shiftForwardUnits,
            final Supplier<Long> loadable) {
        final Long index = nextValueIndexLookupTable
                .getOrLoad(new HashRangeShiftUnitsKey(hashKey, date, shiftForwardUnits), loadable);
        if (index == null) {
            return -1L;
        } else {
            return index;
        }
    }

    public long getOrLoad_previousValueIndexLookupTable(final String hashKey, final FDate date,
            final int shiftBackUnits, final Supplier<Long> loadable) {
        final Long index = previousValueIndexLookupTable
                .getOrLoad(new HashRangeShiftUnitsKey(hashKey, date, shiftBackUnits), loadable);
        if (index == null) {
            return -1L;
        } else {
            return index;
        }
    }

}
