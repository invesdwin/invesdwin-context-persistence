package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.util.collections.iterable.ICloseableIterator;

public interface ITimeSeriesMemoryFileLookupTable {

    void put(MemoryFileSummary summary);

    void deleteRange();

    ICloseableIterator<MemoryFileSummary> range();

    MemoryFileMetadata getMetadata();

}
