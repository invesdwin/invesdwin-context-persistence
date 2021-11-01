package de.invesdwin.context.persistence.jpa.api.bulkinsert.internal;

import java.io.Closeable;
import java.util.List;

public interface IBulkInsertEntities<E> extends Closeable {

    int persist();

    void stage(List<E> entities);

    boolean isDisabledChecks();

    IBulkInsertEntities<E> setDisabledChecks(boolean disabledChecks);

    boolean isSkipPrepareEntities();

    IBulkInsertEntities<E> setSkipPrepareEntities(boolean prepareEntityInEntityManager);

}
