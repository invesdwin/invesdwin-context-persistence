package de.invesdwin.context.persistence.jpa.api.dao;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;
import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.Query;
import javax.persistence.TypedQuery;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.support.SimpleJpaRepository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ReflectionUtils.FieldCallback;

import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.api.query.QueryConfig;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Reflections;

/**
 * A DAO (DataAccessObject) is a special Repository that works for only one Entity. It implements default
 * CRUD-Operations for it.
 * 
 * JPA Entity attached/detaches status is automatically handled, so you don't need to care about that.
 * 
 * @author subes
 */
@ThreadSafe
public abstract class ACustomIdDao<E, PK extends Serializable> extends ARepository implements IDao<E, PK> {

    protected final QueryByExampleHelper<E, PK> queryByExampleHelper;
    private final Class<E> genericType;
    private final boolean deleteInBatchSupported;
    private Integer connectionBatchSize;
    private SimpleJpaRepository<E, PK> delegate;
    private String persistenceUnitName;

    public ACustomIdDao() {
        this.genericType = findGenericType();
        Assertions.assertThat(genericType).isNotNull();
        this.queryByExampleHelper = new QueryByExampleHelper<E, PK>(this);
        this.deleteInBatchSupported = determineDeleteInBatchSupported(genericType);
    }

    private boolean determineDeleteInBatchSupported(final Class<?> genericType) {
        final MutableBoolean deleteInBatchSupported = new MutableBoolean(true);
        Reflections.doWithFields(genericType, new FieldCallback() {
            @Override
            public void doWith(final Field field) {
                if (!deleteInBatchSupported.getValue()) {
                    return;
                } else if (Reflections.getAnnotation(field, ElementCollection.class) != null) {
                    //element collections are mapped as separate tables, thus the values would cause a foreign key constraint violation
                    deleteInBatchSupported.setValue(false);
                } else if (Reflections.getAnnotation(field, Embedded.class) != null) {
                    //check embedded types for the same constraints
                    if (!determineDeleteInBatchSupported(field.getType())) {
                        deleteInBatchSupported.setValue(false);
                    }
                }
            }
        });
        return deleteInBatchSupported.getValue();
    }

    private synchronized SimpleJpaRepository<E, PK> getDelegate() {
        if (delegate == null) {
            delegate = new SimpleJpaRepository<E, PK>(getGenericType(), getEntityManager());
        }
        return delegate;
    }

    private synchronized int getConnectionBatchSize() {
        if (connectionBatchSize == null) {
            connectionBatchSize = PersistenceProperties.getPersistenceUnitContext(getPersistenceUnitName())
                    .getConnectionBatchSize();
        }
        return connectionBatchSize;
    }

    protected final Class<E> getGenericType() {
        return genericType;
    }

    /**
     * @see <a href="http://blog.xebia.com/2009/02/07/acessing-generic-types-at-runtime-in-java/">Source</a>
     */
    @SuppressWarnings("unchecked")
    protected Class<E> findGenericType() {
        return (Class<E>) Reflections.resolveTypeArguments(getClass(), ACustomIdDao.class)[0];
    }

    protected final boolean isDeleteInBatchSupported() {
        return deleteInBatchSupported;
    }

    @Override
    public final synchronized String getPersistenceUnitName() {
        if (persistenceUnitName == null) {
            persistenceUnitName = PersistenceProperties.getPersistenceUnitName(genericType);
            Assertions.assertThat(persistenceUnitName).isNotNull();
        }
        return persistenceUnitName;
    }

    @Override
    public final E findOneFast() {
        return findOneFast(null);
    }

    @SuppressWarnings({ "unchecked", "null" })
    @Override
    public E findOneFast(final QueryConfig config) {
        final Query query = getEntityManager().createQuery("SELECT e FROM " + getGenericType().getName() + " e");
        QueryConfig.configure(getPersistenceUnitName(), query, config);
        final List<E> results = query.setMaxResults(1).getResultList();
        if (results.isEmpty()) {
            return (E) null;
        } else {
            return results.get(0);
        }
    }

    /**
     * Returns a random row from the table.
     */
    @SuppressWarnings("unchecked")
    @Override
    public final E findOneRandom() {
        final List<PK> ids = getEntityManager().createQuery("SELECT e.id FROM " + getGenericType().getName() + " e")
                .getResultList();
        if (ids.size() == 0) {
            return null;
        }
        final int randomId = (int) ((Math.random() * ids.size()));
        return findOneById(ids.get(randomId));
    }

    @Override
    public final E findOneById(final PK id) {
        return findById(id).orElse(null);
    }

    @Override
    public Optional<E> findById(final PK id) {
        return getDelegate().findById(id);
    }

    @Override
    public E getOne(final PK id) {
        return getDelegate().getOne(id);
    }

    @Override
    public final boolean existsById(final PK id) {
        return getDelegate().existsById(id);
    }

    @Override
    public final boolean exists(final E example) {
        final PK id = extractId(example);
        if (id != null) {
            return existsById(id);
        } else {
            return count(example) > 0;
        }
    }

    @Override
    public final E findOne(final E e) {
        return findOne(e, null);
    }

    /**
     * If ID is set: reads or resets the Entity with the data from the DB.
     * 
     * If ID null: invokes a QueryByExample with only one expected result.
     */
    @SuppressWarnings("unchecked")
    @Override
    public final E findOne(final E e, final QueryConfig config) {
        final PK id = extractId(e);
        if (id == null) {
            return (E) getMaxSingleResult(queryByExample(e, true, config));
        } else {
            return findOneById(id);
        }
    }

    /**
     * Reads all entities of this type from DB.
     */
    @Override
    public final List<E> findAll() {
        return getDelegate().findAll();
    }

    /**
     * Reads all entities of this type from DB. With hints and limits.
     */
    @SuppressWarnings("unchecked")
    @Override
    public final List<E> findAll(final QueryConfig config) {
        final Query q = getEntityManager().createQuery("SELECT e FROM " + getGenericType().getName() + " e");
        QueryConfig.configure(getPersistenceUnitName(), q, config);
        return q.getResultList();
    }

    /**
     * Invokes a QueryByExample with all results accepted.
     */
    @Override
    public final List<E> findAll(final E example) {
        return findAll(example, null);
    }

    /**
     * Invokes a QueryByExample with all results accepted. With hints and limits.
     */
    @Override
    public final List<E> findAll(final E example, final QueryConfig hints) {
        final TypedQuery<E> query = queryByExample(example, true, hints);
        QueryConfig.configure(getPersistenceUnitName(), query, hints);
        return query.getResultList();
    }

    @Override
    public final Page<E> findAll(final Pageable pageable) {
        return getDelegate().findAll(pageable);
    }

    @Override
    public final List<E> findAll(final Sort sort) {
        return getDelegate().findAll(sort);
    }

    @Override
    public final List<E> findAllById(final Iterable<PK> ids) {
        return getDelegate().findAllById(ids);
    }

    /**
     * Returns the number of rows in the table.
     */
    @Override
    public final long count() {
        return getDelegate().count();
    }

    /**
     * Returns the number of rows for the QueryByExample.
     */
    @Override
    public final long count(final E example) {
        return count(example, null);
    }

    /**
     * Returns the number of rows for the QueryByExample.
     */
    @Override
    public final long count(final E example, final QueryConfig hints) {
        final Query query = countByExample(example, true);
        QueryConfig.configure(getPersistenceUnitName(), query, hints);
        return (Long) query.getSingleResult();
    }

    /**
     * Creates or updates the Entity in DB. Eqivalent to insert/update, depending on the entities status.
     */
    @Override
    @Transactional
    public final <S extends E> S save(final S e) {
        return getDelegate().save(e);
    }

    @Transactional
    @Override
    public final <S extends E> List<S> saveAll(final Iterable<S> entities) {
        final List<S> saved = new ArrayList<S>();
        final int connectionBatchSize = getConnectionBatchSize();
        int i = 0;
        for (final S entity : entities) {
            saved.add(save(entity));
            if (i++ % connectionBatchSize == 0) {
                flush();
                clear();
            }
        }
        return saved;
    }

    @Override
    @Transactional
    public <S extends E> S saveAndFlush(final S entity) {
        return getDelegate().saveAndFlush(entity);
    }

    @Override
    @Transactional
    public final void delete(final E e) {
        if (getEntityManager().contains(e)) {
            getDelegate().delete(e);
        } else {
            final PK id = extractId(e);
            if (id != null) {
                deleteById(id);
            } else {
                deleteAll(e);
            }
        }

    }

    @Transactional
    @Override
    public void deleteAll() {
        if (isDeleteInBatchSupported()) {
            getDelegate().deleteAllInBatch();
        } else {
            getDelegate().deleteAll();
        }
    }

    @Override
    public final void deleteAllInBatch() {
        deleteAll();
    }

    /**
     * Should be used cautiously, but is useful for tests.
     * 
     * @return the number of deleted rows.
     * 
     * @see <a href=
     *      "http://stackoverflow.com/questions/3037478/is-there-a-way-to-reduce-the-amount-of-boiler-plate-code-associated-with-a-criter/4998083#4998083"
     *      >Diskussion</a>
     */
    @Transactional
    @Override
    public final void deleteAll(final E example) {
        if (isDeleteInBatchSupported()) {
            deleteByExample(example, true).executeUpdate();
        } else {
            deleteAll(findAll(example));
        }
    }

    @Transactional
    @Override
    public void deleteById(final PK id) {
        getDelegate().deleteById(id);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    @Transactional
    public void deleteAll(final Iterable<? extends E> entities) {
        deleteInBatch((Iterable) entities);
    }

    @Override
    @Transactional
    public final void deleteInBatch(final Iterable<E> entities) {
        if (isDeleteInBatchSupported()) {
            getDelegate().deleteInBatch(entities);
        } else {
            getDelegate().deleteAll(entities);
        }
    }

    @Override
    public <S extends E> List<S> findAll(final Example<S> example) {
        return getDelegate().findAll(example);
    }

    @Override
    public <S extends E> List<S> findAll(final Example<S> example, final Sort sort) {
        return getDelegate().findAll(example, sort);
    }

    @Override
    public <S extends E> Optional<S> findOne(final Example<S> example) {
        return getDelegate().findOne(example);
    }

    @Override
    public <S extends E> Page<S> findAll(final Example<S> example, final Pageable pageable) {
        return getDelegate().findAll(example, pageable);
    }

    @Override
    public <S extends E> long count(final Example<S> example) {
        return getDelegate().count(example);
    }

    @Override
    public <S extends E> boolean exists(final Example<S> example) {
        return getDelegate().exists(example);
    }

    /******************* protected **************************************/

    /**
     * When and=false, all criteria are combined with OR, else AND is used.
     * 
     * @see <a href="http://stackoverflow.com/questions/2880209/jpa-findbyexample">Source</a>
     */
    protected final TypedQuery<E> queryByExample(final E example, final boolean and, final QueryConfig config) {
        return queryByExampleHelper.queryByExample(getPersistenceUnitName(), getEntityManager(), getGenericType(),
                example, and, config);
    }

    protected final Query deleteByExample(final E example, final boolean and) {
        return queryByExampleHelper.deleteByExample(getEntityManager(), getGenericType(), example, and);
    }

    protected final Query countByExample(final E example, final boolean and) {
        return queryByExampleHelper.countByExample(getEntityManager(), getGenericType(), example, and);
    }

    @Override
    public boolean isEmpty() {
        return getEntityManager().createQuery("SELECT e FROM " + getGenericType().getName() + " e")
                .setMaxResults(1)
                .getResultList()
                .isEmpty();
    }

}
