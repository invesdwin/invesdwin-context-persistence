package de.invesdwin.context.persistence.jpa.api.dao;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.api.IPersistenceUnitAware;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;

/**
 * You can use Reposities to do operations over multiple DAOs with multiple Entites. Or just to have another class to
 * write queries in, if the DAO cannot be edited, the size of it is too large or if the queries are for special usecases
 * that are not generally needed in other modules.
 * 
 * All methods in child classes automatically have the transaction attribute:
 * 
 * <p>
 * {@literal @}Transactional(readOnly = true, propagation = Propagation.SUPPORTS)
 * </p>
 * 
 * To define a DML method, you have to add the transaction annotation:
 * 
 * <p>
 * {@literal @}Transactional
 * </p>
 * 
 * @author subes
 * 
 */
@ThreadSafe
@Transactional(readOnly = true, propagation = Propagation.SUPPORTS)
public abstract class ARepository implements IPersistenceUnitAware {

    private EntityManager entityManager;

    /**
     * This here is a SharedEntityManager that is thread safe
     * 
     * @see <a href="http://forum.springsource.org/showthread.php?t=33804">Thread-Safety</a>
     */
    protected synchronized EntityManager getEntityManager() {
        if (entityManager == null) {
            final String persistenceUnitName = getPersistenceUnitName();
            entityManager = PersistenceProperties.getPersistenceUnitContext(persistenceUnitName).getEntityManager();
        }
        return entityManager;
    }

    /**
     * Because getSingleResult throws an exception when no result is returned.
     */
    protected final Object getMaxSingleResult(final Query query) {
        final List<?> res = query.getResultList();
        if (res.size() > 1) {
            throw new IncorrectResultSizeDataAccessException(1, res.size());
        } else if (res.size() == 0) {
            return null;
        } else {
            return res.get(0);
        }
    }

    /**
     * May only be used inside a transaction.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public final void flush() {
        getEntityManager().flush();
    }

    public final void clear() {
        getEntityManager().clear();
    }

    public final void detach(final Object o) {
        getEntityManager().detach(o);
    }

    /**
     * Can be overriden to specify a different persistence unit.
     */
    @Override
    public String getPersistenceUnitName() {
        return PersistenceProperties.DEFAULT_PERSISTENCE_UNIT_NAME;
    }

}
