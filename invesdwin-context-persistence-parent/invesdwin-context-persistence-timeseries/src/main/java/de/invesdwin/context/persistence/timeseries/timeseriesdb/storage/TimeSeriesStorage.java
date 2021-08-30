package de.invesdwin.context.persistence.timeseries.timeseriesdb.storage;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.streams.compressor.ICompressorFactory;
import de.invesdwin.context.persistence.timeseries.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseries.ezdb.RangeTablePersistenceMode;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class TimeSeriesStorage {

    private final File directory;
    private final ICompressorFactory compressorFactory;
    private final ADelegateRangeTable<String, FDate, ChunkValue> fileLookupTable;
    private final ADelegateRangeTable<String, FDate, SingleValue> latestValueLookupTable;
    private final ADelegateRangeTable<String, ShiftUnitsRangeKey, SingleValue> previousValueLookupTable;
    private final ADelegateRangeTable<String, ShiftUnitsRangeKey, SingleValue> nextValueLookupTable;

    public TimeSeriesStorage(final File directory, final Integer valueFixedLength,
            final ICompressorFactory compressorFactory) {
        this.directory = directory;
        this.compressorFactory = compressorFactory;
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
        this.nextValueLookupTable = new ADelegateRangeTable<String, ShiftUnitsRangeKey, SingleValue>(
                "nextValueLookupTable") {

            @Override
            protected File getDirectory() {
                return directory;
            }

            @Override
            protected ISerde<ShiftUnitsRangeKey> newRangeKeySerde() {
                return ShiftUnitsRangeKeySerde.GET;
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
        this.previousValueLookupTable = new ADelegateRangeTable<String, ShiftUnitsRangeKey, SingleValue>(
                "previousValueLookupTable") {

            @Override
            protected File getDirectory() {
                return directory;
            }

            @Override
            protected ISerde<ShiftUnitsRangeKey> newRangeKeySerde() {
                return ShiftUnitsRangeKeySerde.GET;
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

    public ICompressorFactory getCompressorFactory() {
        return compressorFactory;
    }

    public ADelegateRangeTable<String, FDate, ChunkValue> getFileLookupTable() {
        return fileLookupTable;
    }

    public ADelegateRangeTable<String, FDate, SingleValue> getLatestValueLookupTable() {
        return latestValueLookupTable;
    }

    public ADelegateRangeTable<String, ShiftUnitsRangeKey, SingleValue> getPreviousValueLookupTable() {
        return previousValueLookupTable;
    }

    public ADelegateRangeTable<String, ShiftUnitsRangeKey, SingleValue> getNextValueLookupTable() {
        return nextValueLookupTable;
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

}
