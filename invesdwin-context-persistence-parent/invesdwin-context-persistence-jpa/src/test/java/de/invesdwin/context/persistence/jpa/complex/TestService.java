package de.invesdwin.context.persistence.jpa.complex;

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
public class TestService implements IPersistenceUnitAware {

    @Inject
    private TestDao dao;

    @Transactional
    public TestEntity saveTestEntity() {
        final TestEntity ent = new TestEntity();
        ent.setName("TestEntity");
        dao.save(ent);
        return ent;
    }

    public TestEntity saveTestEntityWithoutAnnotation() {
        final TestEntity ent = new TestEntity();
        ent.setName("UpNoTransactional");
        dao.save(ent);
        return ent;
    }

    @Transactional
    public TestEntity save(final TestEntity ent) {
        return dao.save(ent);
    }

    public TestEntity getByName(final String name) {
        final TestEntity example = new TestEntity();
        example.setName(name);
        final List<TestEntity> l = dao.findAll(example);
        if (l.size() == 0) {
            return null;
        } else if (l.size() > 1) {
            throw new NonUniqueResultException(String.valueOf(l.size()));
        } else {
            return l.get(0);
        }
    }

    @Transactional
    public void saveEntityAndRollback(final TestEntity ent) {
        dao.save(ent);
        throw new IllegalStateException("Rollback cause");
    }

    @Transactional
    public void saveEntityAndRollbackAfterRequiresNewMethodCall(final TestEntity ent) {
        schreibeEntityRequiresNew(ent);
        throw new IllegalStateException("Rollback cause");
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    private void schreibeEntityRequiresNew(final TestEntity ent) {
        dao.save(ent);
    }

    @Override
    public String getPersistenceUnitName() {
        return "some wrong persistence unit, coz class level transactiona annotation should get parsed";
    }

}
