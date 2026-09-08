package de.invesdwin.context.persistence.timeseriesdb.storage.memory;

public interface ISkipMemoryFileSummaryFunction {

    boolean skipFile(MemoryFileSummary file);

}
