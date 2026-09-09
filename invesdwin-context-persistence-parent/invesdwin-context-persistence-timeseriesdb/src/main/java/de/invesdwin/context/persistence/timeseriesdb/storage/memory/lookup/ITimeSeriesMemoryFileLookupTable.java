package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.util.List;

import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.util.collections.iterable.ICloseableIterator;

public interface ITimeSeriesMemoryFileLookupTable {

    void put(List<MemoryFileSummary> summaries);

    void deleteRange();

    ICloseableIterator<MemoryFileSummary> range();

    MemoryFileMetadata getMetadata();

}
