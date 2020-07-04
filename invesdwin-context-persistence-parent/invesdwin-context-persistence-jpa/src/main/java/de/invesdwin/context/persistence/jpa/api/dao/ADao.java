package de.invesdwin.context.persistence.jpa.api.dao;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.jpa.api.dao.entity.IEntity;
import de.invesdwin.util.lang.reflection.Reflections;

/**
 * A DAO (DataAccessObject) is a special Repository that works for only one Entity. It implements default
 * CRUD-Operations for it.
 * 
 * JPA Entity attached/detaches status is automatically handled, so you don't need to care about that.
 * 
 * @author subes
 */
@ThreadSafe
public abstract class ADao<E extends IEntity> extends ACustomIdDao<E, Long> {

    /**
     * @see <a href="http://blog.xebia.com/2009/02/07/acessing-generic-types-at-runtime-in-java/">Source</a>
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Class<E> findGenericType() {
        return (Class<E>) Reflections.resolveTypeArgument(getClass(), ADao.class);
    }

    public final void deleteIds(final Iterable<Long> ids) {
        queryByExampleHelper.deleteByIds(getEntityManager(), getGenericType(), ids).executeUpdate();
    }

    @Override
    public Long extractId(final E entity) {
        return entity.getId();
    }

}
