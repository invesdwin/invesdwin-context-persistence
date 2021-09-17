package de.invesdwin.context.persistence.timeseriesdb.updater;

import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateFoundException;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;

public interface ITimeSeriesUpdater<K, V> {

    K getKey();

    FDate getMinTime();

    FDate getMaxTime();

    boolean update() throws IncompleteUpdateFoundException;

    Percent getProgress();

}
