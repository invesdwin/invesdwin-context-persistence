package de.invesdwin.context.persistence.timeseries.timeseriesdb.updater;

import de.invesdwin.context.persistence.timeseries.timeseriesdb.IncompleteUpdateFoundException;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.fdate.FDate;

public interface ITimeSeriesUpdater<K, V> {

    K getKey();

    FDate getMinTime();

    FDate getMaxTime();

    boolean update() throws IncompleteUpdateFoundException;

    Percent getProgress();

}
