package de.invesdwin.context.persistence.jpa.api.bulkinsert;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.api.bulkinsert.internal.IBulkInsertEntities;
import de.invesdwin.context.persistence.jpa.api.bulkinsert.internal.JPABatchInsert;
import de.invesdwin.context.persistence.jpa.api.bulkinsert.internal.MySqlLoadDataInfile;
import de.invesdwin.context.persistence.jpa.api.dao.entity.IEntity;
import de.invesdwin.util.assertions.Assertions;

@ThreadSafe
public class BulkInsertEntities<E> implements IBulkInsertEntities<E> {

    private final IBulkInsertEntities<E> delegate;
    private final Class<E> genericType;

    public BulkInsertEntities(final Class<E> genericType) {
        this.genericType = genericType;
        this.delegate = createDelegate(genericType);
    }

    private IBulkInsertEntities<E> createDelegate(final Class<E> genericType) {
        final PersistenceUnitContext puContext = PersistenceProperties.getPersistenceUnitContext(genericType);
        switch (puContext.getConnectionDialect()) {
        case MYSQL:
            return new MySqlLoadDataInfile<E>(genericType, puContext);
        default:
            //e.g. when running in tests
            return new JPABatchInsert<E>(genericType, puContext);
        }
    }

    @Override
    public int persist() {
        return delegate.persist();
    }

    @Override
    public void stage(final List<E> entities) {
        delegate.stage(entities);
    }

    @Override
    public boolean isDisabledChecks() {
        return delegate.isDisabledChecks();
    }

    @Override
    public BulkInsertEntities<E> withDisabledChecks(final boolean disabledChecks) {
        delegate.withDisabledChecks(disabledChecks);
        return this;
    }

    @Override
    public boolean isSkipPrepareEntities() {
        return delegate.isSkipPrepareEntities();
    }

    @Override
    public BulkInsertEntities<E> withSkipPrepareEntities(final boolean skipPrepareEntity) {
        if (skipPrepareEntity) {
            Assertions.assertThat(genericType).isNotInstanceOf(IEntity.class);
        }
        delegate.withSkipPrepareEntities(skipPrepareEntity);
        return this;
    }

    @Override
    public void close() {
        try {
            delegate.close();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
