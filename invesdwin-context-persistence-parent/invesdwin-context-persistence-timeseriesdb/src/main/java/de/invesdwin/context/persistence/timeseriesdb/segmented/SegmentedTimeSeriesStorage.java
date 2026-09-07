package de.invesdwin.context.persistence.timeseriesdb.segmented;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.persistence.timeseriesdb.directory.ITimeSeriesDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.TimeSeriesStorage;

@ThreadSafe
public class SegmentedTimeSeriesStorage extends TimeSeriesStorage {

    public SegmentedTimeSeriesStorage(final ITimeSeriesDirectory directory, final Integer valueFixedLength,
            final ICompressionFactory compressionFactory) {
        super(directory, valueFixedLength, compressionFactory);
    }

}
