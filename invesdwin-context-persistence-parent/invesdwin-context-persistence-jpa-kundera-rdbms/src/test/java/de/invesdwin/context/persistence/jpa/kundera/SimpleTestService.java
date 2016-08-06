package de.invesdwin.context.persistence.jpa.kundera;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Named;
import javax.persistence.NonUniqueResultException;

import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.api.IPersistenceUnitAware;

@Named
@ThreadSafe
@Transactional(value = PersistenceProperties.DEFAULT_TRANSACTION_MANAGER_NAME, propagation = Propagation.NEVER)
public class SimpleTestService implements IPersistenceUnitAware {

    @Inject
    private SimpleTestDao dao;

    @Transactional
    public SimpleTestEntity saveSimpleTestEntity() {
        final SimpleTestEntity ent = new SimpleTestEntity();
        ent.setName("SimpleTestEntity");
        dao.save(ent);
        return ent;
    }

    public SimpleTestEntity saveSimpleTestEntityWithoutAnnotation() {
        final SimpleTestEntity ent = new SimpleTestEntity();
        ent.setName("UpNoTransactional");
        dao.save(ent);
        return ent;
    }

    @Transactional
    public SimpleTestEntity save(final SimpleTestEntity ent) {
        return dao.save(ent);
    }

    public SimpleTestEntity getByName(final String name) {
        final SimpleTestEntity example = new SimpleTestEntity();
        example.setName(name);
        final List<SimpleTestEntity> l = dao.findAll(example);
        if (l.size() == 0) {
            return null;
        } else if (l.size() > 1) {
            throw new NonUniqueResultException(String.valueOf(l.size()));
        } else {
            return l.get(0);
        }
    }

    @Transactional
    public void saveEntityAndRollback(final SimpleTestEntity ent) {
        dao.save(ent);
        throw new IllegalStateException("Rollback cause");
    }

    @Transactional
    public void saveEntityAndRollbackAfterRequiresNewMethodCall(final SimpleTestEntity ent) {
        schreibeEntityRequiresNew(ent);
        throw new IllegalStateException("Rollback cause");
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    private void schreibeEntityRequiresNew(final SimpleTestEntity ent) {
        dao.save(ent);
    }

    @Override
    public String getPersistenceUnitName() {
        return "some wrong persistence unit, coz class level transactiona annotation should get parsed";
    }

}
