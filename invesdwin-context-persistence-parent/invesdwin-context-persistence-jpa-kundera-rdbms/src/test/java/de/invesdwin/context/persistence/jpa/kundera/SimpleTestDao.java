package de.invesdwin.context.persistence.jpa.kundera;

import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.persistence.jpa.api.dao.ARepository;
import de.invesdwin.context.persistence.jpa.api.dao.IDao;
import de.invesdwin.context.persistence.jpa.api.dao.QueryByExampleHelper;
import de.invesdwin.context.persistence.jpa.api.query.QueryConfig;

/**
 * Kundera throws UnsupportedOperationException when Spring-Data-JPA tries to determine if a version field is present in
 * entity, thus we cannot use ADao or Spring-Data-JPA here for now.
 */
@Named
@ThreadSafe
public class SimpleTestDao extends ARepository implements IDao<SimpleTestEntity, Long> {

    private final QueryByExampleHelper<SimpleTestEntity, Long> queryByExampleHelper;

    public SimpleTestDao() {
        this.queryByExampleHelper = new QueryByExampleHelper<SimpleTestEntity, Long>(this) {
            @Override
            protected String extractEntityName(final Class<SimpleTestEntity> genericType) {
                return genericType.getSimpleName();
            }
        };
    }

    @Override
    public EntityManager getEntityManager() {
        return super.getEntityManager();
    }

    @Transactional
    @Override
    public SimpleTestEntity save(final SimpleTestEntity ent) {
        getEntityManager().persist(ent);
        return ent;
    }

    @Transactional
    @Override
    public <S extends SimpleTestEntity> S saveAndFlush(final S ent) {
        save(ent);
        getEntityManager().flush();
        return ent;
    }

    @Transactional
    @Override
    public void deleteAll() {
        getEntityManager().createQuery("DELETE FROM " + SimpleTestEntity.class.getSimpleName() + " e").executeUpdate();
    }

    @Transactional
    @Override
    public List<SimpleTestEntity> findAll(final SimpleTestEntity example) {
        return queryByExample(example, true, null).getResultList();
    }

    @Transactional
    @Override
    public long count() {
        return (Long) getEntityManager()
                .createQuery("SELECT COUNT(e) FROM " + SimpleTestEntity.class.getSimpleName() + " e")
                .getSingleResult();
    }

    @Transactional
    @Override
    public SimpleTestEntity findOneById(final Long id) {
        return (SimpleTestEntity) getMaxSingleResult(getEntityManager()
                .createQuery("SELECT e FROM " + SimpleTestEntity.class.getSimpleName() + " e WHERE e.id = " + id));
    }

    @Transactional
    @Override
    public void delete(final SimpleTestEntity ent) {
        getEntityManager().remove(ent);
    }

    @Transactional
    @Override
    @SuppressWarnings("unchecked")
    public List<SimpleTestEntity> findAll() {
        return getEntityManager().createQuery("SELECT e FROM " + SimpleTestEntity.class.getSimpleName() + " e")
                .getResultList();
    }

    @Transactional
    public void deleteIds(final List<Long> ids) {
        queryByExampleHelper.deleteByIds(getEntityManager(), SimpleTestEntity.class, ids).executeUpdate();
    }

    @Transactional
    @Override
    public void deleteAll(final SimpleTestEntity example) {
        queryByExampleHelper.deleteByExample(getEntityManager(), SimpleTestEntity.class, example, true).executeUpdate();
    }

    @Transactional
    @Override
    public boolean isEmpty() {
        return count() == 0;
    }

    @Transactional
    @Override
    public SimpleTestEntity findOne(final SimpleTestEntity e) {
        final Long id = extractId(e);
        if (id == null) {
            return (SimpleTestEntity) getMaxSingleResult(queryByExample(e, true, null));
        } else {
            return findOneById(id);
        }
    }

    protected final TypedQuery<SimpleTestEntity> queryByExample(final SimpleTestEntity example, final boolean and,
            final QueryConfig config) {
        return queryByExampleHelper.queryByExample(getPersistenceUnitName(), getEntityManager(), SimpleTestEntity.class,
                example, and, config);
    }

    @Transactional
    @Override
    public List<SimpleTestEntity> findAll(final QueryConfig queryConfig) {
        throw new UnsupportedOperationException("TODO");
    }

    @Transactional
    @Override
    public List<SimpleTestEntity> findAll(final Sort sort) {
        throw new UnsupportedOperationException("TODO");
    }

    @Transactional
    @Override
    public void deleteInBatch(final Iterable<SimpleTestEntity> entities) {
        throw new UnsupportedOperationException("TODO");
    }

    @Transactional
    @Override
    public void deleteAllInBatch() {
        throw new UnsupportedOperationException("TODO");
    }

    @Transactional
    @Override
    public SimpleTestEntity getOne(final Long id) {
        throw new UnsupportedOperationException("TODO");
    }

    @Transactional
    @Override
    public Page<SimpleTestEntity> findAll(final Pageable pageable) {
        throw new UnsupportedOperationException("TODO");
    }

    @Transactional
    @Override
    public SimpleTestEntity findOneFast() {
        throw new UnsupportedOperationException("TODO");
    }

    @Transactional
    @Override
    public SimpleTestEntity findOneFast(final QueryConfig config) {
        throw new UnsupportedOperationException("TODO");
    }

    @Transactional
    @Override
    public SimpleTestEntity findOneRandom() {
        throw new UnsupportedOperationException("TODO");
    }

    @Transactional
    @Override
    public SimpleTestEntity findOne(final SimpleTestEntity e, final QueryConfig config) {
        throw new UnsupportedOperationException("TODO");
    }

    @Transactional
    @Override
    public List<SimpleTestEntity> findAll(final SimpleTestEntity example, final QueryConfig hints) {
        throw new UnsupportedOperationException("TODO");
    }

    @Transactional
    @Override
    public long count(final SimpleTestEntity example) {
        throw new UnsupportedOperationException("TODO");
    }

    @Transactional
    @Override
    public long count(final SimpleTestEntity example, final QueryConfig config) {
        throw new UnsupportedOperationException("TODO");
    }

    @Transactional
    @Override
    public boolean exists(final SimpleTestEntity example) {
        throw new UnsupportedOperationException("TODO");
    }

    @Transactional
    @Override
    public Long extractId(final SimpleTestEntity entity) {
        return entity.getId();
    }

    @Override
    public <S extends SimpleTestEntity> List<S> findAll(final Example<S> example) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <S extends SimpleTestEntity> List<S> findAll(final Example<S> example, final Sort sort) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <S extends SimpleTestEntity> Page<S> findAll(final Example<S> example, final Pageable pageable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <S extends SimpleTestEntity> long count(final Example<S> example) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <S extends SimpleTestEntity> boolean exists(final Example<S> example) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SimpleTestEntity> findAllById(final Iterable<Long> ids) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public <S extends SimpleTestEntity> List<S> saveAll(final Iterable<S> entities) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Optional<SimpleTestEntity> findById(final Long id) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public boolean existsById(final Long id) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void deleteById(final Long id) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void deleteAll(final Iterable<? extends SimpleTestEntity> entities) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public <S extends SimpleTestEntity> Optional<S> findOne(final Example<S> example) {
        throw new UnsupportedOperationException("TODO");
    }

}
