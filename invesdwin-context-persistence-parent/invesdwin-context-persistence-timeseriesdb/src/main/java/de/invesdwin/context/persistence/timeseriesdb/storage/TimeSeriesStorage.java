package de.invesdwin.context.persistence.timeseriesdb.storage;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.persistentmap.APersistentMap;
import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.context.integration.streams.compressor.ICompressionFactory;
import de.invesdwin.context.integration.streams.compressor.lz4.FastLZ4CompressionFactory;
import de.invesdwin.context.persistence.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.ezdb.RangeTablePersistenceMode;
import de.invesdwin.context.persistence.timeseriesdb.TimeseriesProperties;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.RemoteFastSerializingSerde;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class TimeSeriesStorage {

    private final File directory;
    private final ICompressionFactory compressionFactory;
    private final ADelegateRangeTable<String, FDate, ChunkValue> fileLookupTable;
    private final APersistentMap<Pair<String, FDate>, SingleValue> latestValueLookupTable;
    private final APersistentMap<ShiftUnitsHashKey, SingleValue> previousValueLookupTable;
    private final APersistentMap<ShiftUnitsHashKey, SingleValue> nextValueLookupTable;

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
        this.latestValueLookupTable = new APersistentMap<Pair<String, FDate>, SingleValue>("latestValueLookupTable") {

            @Override
            public File getDirectory() {
                return directory;
            }

            @Override
            public ISerde<SingleValue> newValueSerde() {
                return FastLZ4CompressionFactory.INSTANCE.maybeWrap(SingleValueSerde.GET);
            }

            @Override
            public ISerde<Pair<String, FDate>> newKeySerde() {
                return new RemoteFastSerializingSerde<>(false, Pair.class, String.class, FDate.class);
            }

            //            @Override
            //            protected void onDeleteTableFinished() {
            //                throw new CorruptedTimeSeriesStorageException(getName());
            //            }

            @Override
            protected IPersistentMapFactory<Pair<String, FDate>, SingleValue> newFactory() {
                return TimeseriesProperties.newPersistentMapFactory();
            }

        };
        this.nextValueLookupTable = new APersistentMap<ShiftUnitsHashKey, SingleValue>("nextValueLookupTable") {

            @Override
            public File getDirectory() {
                return directory;
            }

            @Override
            public ISerde<ShiftUnitsHashKey> newKeySerde() {
                return ShiftUnitsHashKeySerde.GET;
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
            protected IPersistentMapFactory<ShiftUnitsHashKey, SingleValue> newFactory() {
                return TimeseriesProperties.newPersistentMapFactory();
            }
        };
        this.previousValueLookupTable = new APersistentMap<ShiftUnitsHashKey, SingleValue>("previousValueLookupTable") {

            @Override
            public File getDirectory() {
                return directory;
            }

            @Override
            public ISerde<ShiftUnitsHashKey> newKeySerde() {
                return ShiftUnitsHashKeySerde.GET;
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
            protected IPersistentMapFactory<ShiftUnitsHashKey, SingleValue> newFactory() {
                return TimeseriesProperties.newPersistentMapFactory();
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

    public APersistentMap<Pair<String, FDate>, SingleValue> getLatestValueLookupTable() {
        return latestValueLookupTable;
    }

    public APersistentMap<ShiftUnitsHashKey, SingleValue> getPreviousValueLookupTable() {
        return previousValueLookupTable;
    }

    public APersistentMap<ShiftUnitsHashKey, SingleValue> getNextValueLookupTable() {
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
