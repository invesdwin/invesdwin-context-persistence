package de.invesdwin.context.persistence.jpa.eclipselink;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.jpa.api.dao.ADao;
import jakarta.inject.Named;
import jakarta.persistence.EntityManager;

@Named
@ThreadSafe
public class SimpleTestDao extends ADao<SimpleTestEntity> {

    @Override
    public EntityManager getEntityManager() {
        return super.getEntityManager();
    }

}
