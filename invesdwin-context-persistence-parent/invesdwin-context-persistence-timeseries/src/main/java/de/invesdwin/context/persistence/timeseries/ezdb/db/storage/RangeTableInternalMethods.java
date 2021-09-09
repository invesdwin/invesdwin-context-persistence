package de.invesdwin.context.persistence.timeseries.ezdb.db.storage;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.timeseries.ezdb.EzdbSerde;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import ezdb.RawTableRow;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Immutable
public class RangeTableInternalMethods {

    private final ISerde hashKeySerde;
    private final ISerde rangeKeySerde;
    private final ISerde valueSerde;
    private final Comparator<byte[]> hashKeyComparatorDisk;
    private final Comparator<byte[]> rangeKeyComparatorDisk;
    private final Comparator<Object> hashKeyComparatorMemory;
    private final Comparator<Object> rangeKeyComparatorMemory;
    private final File directory;

    public RangeTableInternalMethods(final ISerde hashKeySerde, final ISerde rangeKeySerde, final ISerde valueSerde,
            final Comparator<byte[]> hashKeyComparatorDisk, final Comparator<byte[]> rangeKeyComparatorDisk,
            final Comparator<Object> hashKeyComparatorMemory, final Comparator<Object> rangeKeyComparatorMemory,
            final File directory) {
        this.hashKeySerde = hashKeySerde;
        this.rangeKeySerde = rangeKeySerde;
        this.valueSerde = valueSerde;
        this.hashKeyComparatorDisk = hashKeyComparatorDisk;
        this.rangeKeyComparatorDisk = rangeKeyComparatorDisk;
        this.hashKeyComparatorMemory = hashKeyComparatorMemory;
        this.rangeKeyComparatorMemory = rangeKeyComparatorMemory;
        this.directory = directory;
    }

    public ISerde getHashKeySerde() {
        return hashKeySerde;
    }

    public ISerde getRangeKeySerde() {
        return rangeKeySerde;
    }

    public ISerde getValueSerde() {
        return valueSerde;
    }

    public Comparator<byte[]> getHashKeyComparatorDisk() {
        return hashKeyComparatorDisk;
    }

    public Comparator<byte[]> getRangeKeyComparatorDisk() {
        return rangeKeyComparatorDisk;
    }

    public Comparator<Object> getHashKeyComparatorMemory() {
        return hashKeyComparatorMemory;
    }

    public Comparator<Object> getRangeKeyComparatorMemory() {
        return rangeKeyComparatorMemory;
    }

    public File getDirectory() {
        return directory;
    }

    public void validateRow(final Entry<byte[], byte[]> rawRow) {
        //fst library might have been updated, in that case deserialization might fail
        final RawTableRow row = new RawTableRow(rawRow, EzdbSerde.valueOf(hashKeySerde),
                EzdbSerde.valueOf(rangeKeySerde), EzdbSerde.valueOf(valueSerde));
        row.getHashKey();
        row.getRangeKey();
        row.getValue();
    }

    public void initDirectory() {
        try {
            Files.forceMkdir(directory);
        } catch (final IOException e) {
            throw Err.process(e);
        }
    }

}
