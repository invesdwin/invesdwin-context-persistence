package de.invesdwin.context.persistence.timeseries.timeseriesdb.storage;

public interface ISkipFileFunction {

    boolean skipFile(ChunkValue file);

}
