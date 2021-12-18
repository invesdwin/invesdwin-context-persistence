package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.persistence.jpa.complex.TestDao;
import de.invesdwin.context.persistence.jpa.complex.TestEntity;

@NotThreadSafe
@Configurable
public class TestDaoPerformanceTransactionalAspectMethods {

    private static final int FIND_ITERATIONS = 5000;

    @Inject
    private TestDao dao;

    @Transactional(value = PersistenceProperties.DEFAULT_TRANSACTION_MANAGER_NAME)
    public void testReadNormal() {
        final Long id = dao.findOneRandom().getId();
        for (int i = 0; i < FIND_ITERATIONS; i++) {
            TestEntity findById = new TestEntity();
            findById.setId(id);
            findById = dao.findOne(findById);
        }
    }

    /**
     * every read a select, just the same as pride
     */
    public void testReadWithDetach() {
        final TestEntity findOneRandom = dao.findOneRandom();
        final Long id = findOneRandom.getId();
        for (int i = 0; i < FIND_ITERATIONS; i++) {
            TestEntity findById = new TestEntity();
            findById.setId(id);
            findById = dao.findOne(findById);
        }
    }

    @Transactional(value = PersistenceProperties.DEFAULT_TRANSACTION_MANAGER_NAME)
    public void testReadWithSerialization() {
        final TestEntity findOneRandom = dao.findOneRandom();
        final Long id = findOneRandom.getId();
        for (int i = 0; i < FIND_ITERATIONS; i++) {
            TestEntity findById = new TestEntity();
            findById.setId(id);
            findById = (TestEntity) dao.findOne(findById).clone();
        }
    }

    public void testNestedTransactions() {
        //need to test max connection pool size here
        recursiveTransaction(0, 20); //100 seems to have a hard limit...
    }

    @Transactional(value = PersistenceProperties.DEFAULT_TRANSACTION_MANAGER_NAME, propagation = Propagation.REQUIRES_NEW)
    private void recursiveTransaction(final int level, final int maxLevel) {
        if (level < maxLevel) {
            recursiveTransaction(level + 1, maxLevel);
        } else {
            testReadNormal();
        }
    }

    @Transactional(value = PersistenceProperties.DEFAULT_TRANSACTION_MANAGER_NAME)
    public void testBatchInsert() {
        for (int i = 0; i < 100; i++) {
            final TestEntity newEntity = new TestEntity();
            newEntity.setName("asdf" + i);
            dao.save(newEntity);
        }
    }

}