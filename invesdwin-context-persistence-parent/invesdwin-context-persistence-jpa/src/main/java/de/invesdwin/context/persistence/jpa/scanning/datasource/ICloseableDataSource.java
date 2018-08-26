package de.invesdwin.context.persistence.jpa.scanning.datasource;

import java.io.Closeable;

import javax.sql.DataSource;

public interface ICloseableDataSource extends DataSource, Closeable {

    @Override
    void close();

}
