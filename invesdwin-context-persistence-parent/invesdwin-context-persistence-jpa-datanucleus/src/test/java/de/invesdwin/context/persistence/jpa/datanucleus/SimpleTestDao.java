package de.invesdwin.context.persistence.jpa.datanucleus;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;
import javax.persistence.EntityManager;

import de.invesdwin.context.persistence.jpa.api.dao.ADao;

@Named
@ThreadSafe
public class SimpleTestDao extends ADao<SimpleTestEntity> {

    @Override
    public EntityManager getEntityManager() {
        return super.getEntityManager();
    }

}
