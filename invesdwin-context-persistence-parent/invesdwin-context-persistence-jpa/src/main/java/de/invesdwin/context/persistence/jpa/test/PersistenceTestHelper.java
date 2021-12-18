package de.invesdwin.context.persistence.jpa.test;

import java.util.Collection;

import javax.annotation.concurrent.NotThreadSafe;
import javax.persistence.PersistenceException;
import javax.persistence.TransactionRequiredException;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.persistence.jpa.api.util.SqlErr;

@NotThreadSafe
public class PersistenceTestHelper implements IPersistenceTestHelper {

    @Override
    //    @Transactional(value = PersistenceProperties.DEFAULT_TRANSACTION_MANAGER_NAME, propagation = Propagation.NEVER)
    public void clearAllTables() {
        boolean retry = false;
        final Collection<IClearAllTablesAware> allDaos = MergedContext.getInstance()
                .getBeansOfType(IClearAllTablesAware.class)
                .values();
        for (final IClearAllTablesAware dao : allDaos) {
            try {
                dao.deleteAll();
            } catch (final TransactionRequiredException e) {
                throw e;
            } catch (final PersistenceException e) {
                /*
                 * ConstraintViolation because of foreign keys, thus retry
                 */
                SqlErr.logSqlException(new RuntimeException("Retrying deletion of " + dao.getClass().getSimpleName()
                        + ". Only circular dependencies of foreign keys are a problem here and should be fixed by overriding the deleteAll method inside the DAO.",
                        e));
                retry = true;
            }
        }
        if (retry) {
            clearAllTables();
        }
    }
}
