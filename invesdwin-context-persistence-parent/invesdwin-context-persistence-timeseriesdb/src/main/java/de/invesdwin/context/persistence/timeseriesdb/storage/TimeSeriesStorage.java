package de.invesdwin.context.persistence.timeseriesdb.storage;

import java.io.File;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.streams.compressor.ICompressionFactory;
import de.invesdwin.context.persistence.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.ezdb.RangeTablePersistenceMode;
import de.invesdwin.context.persistence.timeseriesdb.storage.key.RangeShiftUnitsKey;
import de.invesdwin.context.persistence.timeseriesdb.storage.key.RangeShiftUnitsKeySerde;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class TimeSeriesStorage {

    private final File directory;
    private final ICompressionFactory compressionFactory;
    private final ADelegateRangeTable<String, FDate, ChunkValue> fileLookupTable;
    private final ADelegateRangeTable<String, FDate, SingleValue> latestValueLookupTable;
    private final ADelegateRangeTable<String, RangeShiftUnitsKey, SingleValue> previousValueLookupTable;
    private final ADelegateRangeTable<String, RangeShiftUnitsKey, SingleValue> nextValueLookupTable;

    public TimeSeriesStorage(final File directory, final Integer valueFixedLength,
            final ICompressionFactory compressionFactory) {
        this.directory = directory;
        this.compressionFactory = compressionFactory;
        this.fileLookupTable = new ADelegateRangeTable<String, FDate, ChunkValue>("fileLookupTable") {

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
            protected ISerde<ChunkValue> newValueSerde() {
                return new ChunkValueSerde(valueFixedLength);
            }

            @Override
            protected RangeTablePersistenceMode getPersistenceMode() {
                return RangeTablePersistenceMode.MEMORY_WRITE_THROUGH_DISK;
            }

        };
        this.latestValueLookupTable = new ADelegateRangeTable<String, FDate, SingleValue>("latestValueLookupTable") {

            @Override
            protected File getDirectory() {
                return directory;
            }

            @Override
            protected ISerde<SingleValue> newValueSerde() {
                return SingleValueSerde.GET;
            }

            @Override
            protected void onDeleteTableFinished() {
                throw new CorruptedTimeSeriesStorageException(getName());
            }

        };
        this.nextValueLookupTable = new ADelegateRangeTable<String, RangeShiftUnitsKey, SingleValue>(
                "nextValueLookupTable") {

            @Override
            protected File getDirectory() {
                return directory;
            }

            @Override
            protected ISerde<RangeShiftUnitsKey> newRangeKeySerde() {
                return RangeShiftUnitsKeySerde.GET;
            }

            @Override
            protected ISerde<SingleValue> newValueSerde() {
                return SingleValueSerde.GET;
            }

            @Override
            protected void onDeleteTableFinished() {
                throw new CorruptedTimeSeriesStorageException(getName());
            }
        };
        this.previousValueLookupTable = new ADelegateRangeTable<String, RangeShiftUnitsKey, SingleValue>(
                "previousValueLookupTable") {

            @Override
            protected File getDirectory() {
                return directory;
            }

            @Override
            protected ISerde<RangeShiftUnitsKey> newRangeKeySerde() {
                return RangeShiftUnitsKeySerde.GET;
            }

            @Override
            protected ISerde<SingleValue> newValueSerde() {
                return SingleValueSerde.GET;
            }

            @Override
            protected void onDeleteTableFinished() {
                throw new CorruptedTimeSeriesStorageException(getName());
            }
        };
    }

    public File getDirectory() {
        return directory;
    }

    public ICompressionFactory getCompressionFactory() {
        return compressionFactory;
    }

    public ADelegateRangeTable<String, FDate, ChunkValue> getFileLookupTable() {
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
        latestValueLookupTable.deleteRange(hashKey);
    }

    public void deleteRange_latestValueLookupTable(final String hashKey, final FDate above) {
        if (above == null) {
            deleteRange_latestValueLookupTable(hashKey);
        } else {
            latestValueLookupTable.deleteRange(hashKey, above);
        }
    }

    public void deleteRange_nextValueLookupTable(final String hashKey) {
        nextValueLookupTable.deleteRange(hashKey);
    }

    public void deleteRange_previousValueLookupTable(final String hashKey) {
        previousValueLookupTable.deleteRange(hashKey);
    }

    public void deleteRange_previousValueLookupTable(final String hashKey, final FDate above) {
        if (above == null) {
            deleteRange_previousValueLookupTable(hashKey);
        } else {
            previousValueLookupTable.deleteRange(hashKey, new RangeShiftUnitsKey(above, 0));
        }
    }

    public SingleValue getOrLoad_latestValueLookupTable(final String hashKey, final FDate key,
            final Supplier<SingleValue> loadable) {
        return latestValueLookupTable.getOrLoad(hashKey, key, loadable);
    }

    public SingleValue getOrLoad_nextValueLookupTable(final String hashKey, final FDate date,
            final int shiftForwardUnits, final Supplier<SingleValue> loadable) {
        return nextValueLookupTable.getOrLoad(hashKey, new RangeShiftUnitsKey(date, shiftForwardUnits), loadable);
    }

    public SingleValue getOrLoad_previousValueLookupTable(final String hashKey, final FDate date,
            final int shiftBackUnits, final Supplier<SingleValue> loadable) {
        return previousValueLookupTable.getOrLoad(hashKey, new RangeShiftUnitsKey(date, shiftBackUnits), loadable);
    }

}
