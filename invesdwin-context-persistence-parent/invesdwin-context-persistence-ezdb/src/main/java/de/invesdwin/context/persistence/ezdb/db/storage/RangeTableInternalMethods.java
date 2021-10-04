package de.invesdwin.context.persistence.ezdb.db.storage;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.ezdb.EzdbSerde;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import ezdb.table.RawTableRow;
import ezdb.table.range.RawRangeTableRow;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Immutable
public class RangeTableInternalMethods {

    private final ISerde hashKeySerde;
    private final ISerde rangeKeySerde;
    private final ISerde valueSerde;
    private final Comparator<java.nio.ByteBuffer> hashKeyComparatorDisk;
    private final Comparator<java.nio.ByteBuffer> rangeKeyComparatorDisk;
    private final Comparator<Object> hashKeyComparatorMemory;
    private final Comparator<Object> rangeKeyComparatorMemory;
    private final File directory;

    public RangeTableInternalMethods(final ISerde hashKeySerde, final ISerde rangeKeySerde, final ISerde valueSerde,
            final Comparator<java.nio.ByteBuffer> hashKeyComparatorDisk,
            final Comparator<java.nio.ByteBuffer> rangeKeyComparatorDisk,
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

    public Comparator<java.nio.ByteBuffer> getHashKeyComparatorDisk() {
        return hashKeyComparatorDisk;
    }

    public Comparator<java.nio.ByteBuffer> getRangeKeyComparatorDisk() {
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

    public void validateRowBuffer(final Entry<java.nio.ByteBuffer, java.nio.ByteBuffer> rawRow,
            final boolean rangeTable) {
        //fst library might have been updated, in that case deserialization might fail
        if (rangeTable) {
            final RawRangeTableRow row = RawRangeTableRow.valueOfBuffer(rawRow, EzdbSerde.valueOf(hashKeySerde),
                    EzdbSerde.valueOf(rangeKeySerde), EzdbSerde.valueOf(valueSerde));
            validateRow(row);
        } else {
            final RawTableRow row = RawTableRow.valueOfBuffer(rawRow, EzdbSerde.valueOf(hashKeySerde),
                    EzdbSerde.valueOf(valueSerde));
            validateRow(row);
        }
    }

    public void validateRowBytes(final Entry<byte[], byte[]> rawRow, final boolean rangeTable) {
        //fst library might have been updated, in that case deserialization might fail
        if (rangeTable) {
            final RawRangeTableRow row = RawRangeTableRow.valueOfBytes(rawRow, EzdbSerde.valueOf(hashKeySerde),
                    EzdbSerde.valueOf(rangeKeySerde), EzdbSerde.valueOf(valueSerde));
            validateRow(row);
        } else {
            final RawTableRow row = RawTableRow.valueOfBytes(rawRow, EzdbSerde.valueOf(hashKeySerde),
                    EzdbSerde.valueOf(valueSerde));
            validateRow(row);
        }
    }

    public void validateRow(final RawRangeTableRow row) {
        row.getHashKey();
        row.getRangeKey();
        row.getValue();
    }

    public void validateRow(final RawTableRow row) {
        row.getHashKey();
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
