package de.invesdwin.context.persistence.jpa.complex;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;
import javax.persistence.EntityManager;

import de.invesdwin.context.persistence.jpa.api.dao.ADao;

@Named
@ThreadSafe
public class TestDao extends ADao<TestEntity> {

    @Override
    public EntityManager getEntityManager() {
        return super.getEntityManager();
    }

}
