package de.invesdwin.context.persistence.jpa.complex;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.jpa.api.dao.ADao;
import jakarta.inject.Named;
import jakarta.persistence.EntityManager;

@Named
@ThreadSafe
public class TestDao extends ADao<TestEntity> {

    @Override
    public EntityManager getEntityManager() {
        return super.getEntityManager();
    }

}
