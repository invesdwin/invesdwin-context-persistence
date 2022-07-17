package de.invesdwin.context.persistence.timeseriesdb.storage;

import java.io.File;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.persistentmap.APersistentMap;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.context.integration.streams.compression.ICompressionFactory;
import de.invesdwin.context.integration.streams.compression.lz4.FastLZ4CompressionFactory;
import de.invesdwin.context.persistence.ezdb.RangeTablePersistenceMode;
import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseriesdb.PersistentMapType;
import de.invesdwin.context.persistence.timeseriesdb.storage.key.HashRangeKey;
import de.invesdwin.context.persistence.timeseriesdb.storage.key.HashRangeKeySerde;
import de.invesdwin.context.persistence.timeseriesdb.storage.key.HashRangeShiftUnitsKey;
import de.invesdwin.context.persistence.timeseriesdb.storage.key.HashRangeShiftUnitsKeySerde;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class TimeSeriesStorage {

    private static final PersistentMapType MAP_TYPE = PersistentMapType.FAST;
    private final File directory;
    private final ICompressionFactory compressionFactory;
    private final ADelegateRangeTable<String, FDate, MemoryFileSummary> fileLookupTable;
    private final APersistentMap<HashRangeKey, SingleValue> latestValueLookupTable;
    private final APersistentMap<HashRangeShiftUnitsKey, SingleValue> previousValueLookupTable;
    private final APersistentMap<HashRangeShiftUnitsKey, SingleValue> nextValueLookupTable;

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
                throw new CorruptedTimeSeriesStorageException(getName());
            }

            @Override
            protected ISerde<MemoryFileSummary> newValueSerde() {
                return new MemoryFileSummarySerde(valueFixedLength);
            }

            @Override
            protected RangeTablePersistenceMode getPersistenceMode() {
                return RangeTablePersistenceMode.DISK_ONLY;
            }

        };
        this.latestValueLookupTable = new APersistentMap<HashRangeKey, SingleValue>("latestValueLookupTable") {

            @Override
            public File getDirectory() {
                return directory;
            }

            @Override
            public ISerde<SingleValue> newValueSerde() {
                return FastLZ4CompressionFactory.INSTANCE.maybeWrap(SingleValueSerde.GET);
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
            protected IPersistentMapFactory<HashRangeKey, SingleValue> newFactory() {
                return MAP_TYPE.newFactory();
            }

        };
        this.nextValueLookupTable = new APersistentMap<HashRangeShiftUnitsKey, SingleValue>("nextValueLookupTable") {

            @Override
            public File getDirectory() {
                return directory;
            }

            @Override
            public ISerde<HashRangeShiftUnitsKey> newKeySerde() {
                return HashRangeShiftUnitsKeySerde.GET;
            }

            @Override
            public ISerde<SingleValue> newValueSerde() {
                return FastLZ4CompressionFactory.INSTANCE.maybeWrap(SingleValueSerde.GET);
            }

            //            @Override
            //            protected void onDeleteTableFinished() {
            //                throw new CorruptedTimeSeriesStorageException(getName());
            //            }

            @Override
            protected IPersistentMapFactory<HashRangeShiftUnitsKey, SingleValue> newFactory() {
                return MAP_TYPE.newFactory();
            }
        };
        this.previousValueLookupTable = new APersistentMap<HashRangeShiftUnitsKey, SingleValue>(
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
            public ISerde<SingleValue> newValueSerde() {
                return FastLZ4CompressionFactory.INSTANCE.maybeWrap(SingleValueSerde.GET);
            }

            //            @Override
            //            protected void onDeleteTableFinished() {
            //                throw new CorruptedTimeSeriesStorageException(getName());
            //            }

            @Override
            protected IPersistentMapFactory<HashRangeShiftUnitsKey, SingleValue> newFactory() {
                return MAP_TYPE.newFactory();
            }
        };
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
        latestValueLookupTable.close();
        previousValueLookupTable.close();
        nextValueLookupTable.close();
    }

    public File newDataDirectory(final String hashKey) {
        return new File(getDirectory(), "storage/" + hashKey);
    }

    public void deleteRange_latestValueLookupTable(final String hashKey) {
        if (MAP_TYPE == PersistentMapType.FAST) {
            //chronicle map does not really support deleting entries, the file size gets bloaded which causes significant I/O
            if (!latestValueLookupTable.isEmpty()) {
                latestValueLookupTable.deleteTable();
            }
        } else {
            latestValueLookupTable.removeAll((key) -> {
                return hashKey.equals(key.getHashKey());
            });
        }
    }

    public void deleteRange_latestValueLookupTable(final String hashKey, final FDate above) {
        if (above == null) {
            deleteRange_latestValueLookupTable(hashKey);
        } else {
            if (MAP_TYPE == PersistentMapType.FAST) {
                //chronicle map does not really support deleting entries, the file size gets bloaded which causes significant I/O
                if (!latestValueLookupTable.isEmpty()) {
                    latestValueLookupTable.deleteTable();
                }
            } else {
                latestValueLookupTable.removeAll((key) -> {
                    return hashKey.equals(key.getHashKey()) && key.getRangeKey().isAfterOrEqualToNotNullSafe(above);
                });
            }
        }
    }

    public void deleteRange_nextValueLookupTable(final String hashKey) {
        if (MAP_TYPE == PersistentMapType.FAST) {
            //chronicle map does not really support deleting entries, the file size gets bloaded which causes significant I/O
            if (!latestValueLookupTable.isEmpty()) {
                nextValueLookupTable.deleteTable();
            }
        } else {
            nextValueLookupTable.removeAll((key) -> {
                return hashKey.equals(key.getHashKey());
            });
        }
    }

    public void deleteRange_previousValueLookupTable(final String hashKey) {
        if (MAP_TYPE == PersistentMapType.FAST) {
            //chronicle map does not really support deleting entries, the file size gets bloaded which causes significant I/O
            if (!previousValueLookupTable.isEmpty()) {
                previousValueLookupTable.deleteTable();
            }
        } else {
            previousValueLookupTable.removeAll((key) -> {
                return hashKey.equals(key.getHashKey());
            });
        }
    }

    public void deleteRange_previousValueLookupTable(final String hashKey, final FDate above) {
        if (above == null) {
            deleteRange_previousValueLookupTable(hashKey);
        } else {
            if (MAP_TYPE == PersistentMapType.FAST) {
                //chronicle map does not really support deleting entries, the file size gets bloaded which causes significant I/O
                if (!previousValueLookupTable.isEmpty()) {
                    previousValueLookupTable.deleteTable();
                }
            } else {
                previousValueLookupTable.removeAll((key) -> {
                    return hashKey.equals(key.getHashKey()) && key.getRangeKey().isAfterOrEqualToNotNullSafe(above);
                });
            }
        }
    }

    public SingleValue getOrLoad_latestValueLookupTable(final String hashKey, final FDate key,
            final Supplier<SingleValue> loadable) {
        return latestValueLookupTable.getOrLoad(new HashRangeKey(hashKey, key), loadable);
    }

    public SingleValue getOrLoad_nextValueLookupTable(final String hashKey, final FDate date,
            final int shiftForwardUnits, final Supplier<SingleValue> loadable) {
        return nextValueLookupTable.getOrLoad(new HashRangeShiftUnitsKey(hashKey, date, shiftForwardUnits), loadable);
    }

    public SingleValue getOrLoad_previousValueLookupTable(final String hashKey, final FDate date,
            final int shiftBackUnits, final Supplier<SingleValue> loadable) {
        return previousValueLookupTable.getOrLoad(new HashRangeShiftUnitsKey(hashKey, date, shiftBackUnits), loadable);
    }

}
