package de.invesdwin.context.persistence.jpa.api.bulkinsert.internal;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.springframework.data.jpa.repository.support.SimpleJpaRepository;
import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.api.dao.DisabledCrudMethodMetadata;
import jakarta.persistence.EntityManager;

@ThreadSafe
public class JPABatchInsert<E> implements IBulkInsertEntities<E> {

    private final EntityManager em;
    private final SimpleJpaRepository<E, Long> delegate;
    private final int connectionBatchSize;
    @GuardedBy("staged")
    private final List<E> staged = new ArrayList<E>();

    public JPABatchInsert(final Class<E> genericType, final PersistenceUnitContext puContext) {
        this.em = puContext.getEntityManager();
        this.delegate = new SimpleJpaRepository<E, Long>(genericType, em);
        this.delegate.setRepositoryMethodMetadata(DisabledCrudMethodMetadata.INSTANCE);
        this.connectionBatchSize = puContext.getConnectionBatchSize();
    }

    @Override
    public JPABatchInsert<E> setDisabledChecks(final boolean disabledChecks) {
        return this;
    }

    @Override
    public boolean isDisabledChecks() {
        return false;
    }

    @Override
    public boolean isSkipPrepareEntities() {
        return false;
    }

    @Override
    public IBulkInsertEntities<E> setSkipPrepareEntities(final boolean skipPrepareEntities) {
        return this;
    }

    @Override
    public void stage(final List<E> entities) {
        synchronized (staged) {
            staged.addAll(entities);
        }
    }

    @Override
    public int persist() {
        synchronized (staged) {
            return internalPersist();
        }
    }

    @SuppressWarnings("GuardedBy")
    @Transactional
    private int internalPersist() {
        int i = 0;
        for (final E entity : staged) {
            delegate.save(entity);
            if (i++ % connectionBatchSize == 0) {
                em.flush();
                em.clear();
            }
        }
        close();
        return i;
    }

    @Override
    public void close() {
        synchronized (staged) {
            staged.clear();
        }
    }

}
