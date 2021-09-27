package de.invesdwin.context.persistence.timeseriesdb.storage;

public interface ISkipFileFunction {

    boolean skipFile(MemoryFileSummary file);

}
