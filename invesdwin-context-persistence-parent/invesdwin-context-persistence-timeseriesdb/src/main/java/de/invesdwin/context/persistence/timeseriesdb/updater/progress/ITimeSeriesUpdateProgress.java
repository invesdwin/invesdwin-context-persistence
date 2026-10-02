package de.invesdwin.context.persistence.timeseriesdb.updater.progress;

import de.invesdwin.util.time.date.FDate;

public interface ITimeSeriesUpdateProgress {

    String getOwner();

    long getValueCount();

    FDate getMaxTime();

    FDate getMinTime();

}
