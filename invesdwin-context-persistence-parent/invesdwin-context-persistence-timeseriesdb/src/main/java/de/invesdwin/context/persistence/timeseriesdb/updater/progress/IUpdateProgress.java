package de.invesdwin.context.persistence.timeseriesdb.updater.progress;

import de.invesdwin.util.time.date.FDate;

public interface IUpdateProgress<K, V> {

    int getValueCount();

    FDate getMaxTime();

    FDate getMinTime();

}
