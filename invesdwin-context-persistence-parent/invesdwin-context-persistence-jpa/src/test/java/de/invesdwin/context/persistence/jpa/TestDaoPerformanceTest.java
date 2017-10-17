package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import org.junit.Test;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.persistence.jpa.complex.TestDao;
import de.invesdwin.context.persistence.jpa.complex.TestEntity;
import de.invesdwin.context.persistence.jpa.test.APersistenceTest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.assertions.Executable;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;

@ThreadSafe
//@ContextConfiguration(locations = { APersistenzTest.CTX_TEST_SERVER }, inheritLocations = false)
public class TestDaoPerformanceTest extends APersistenceTest {

    private static final int FIND_ITERATIONS = 5000;

    @Inject
    private TestDao dao;

    @Override
    public void setUpOnce() throws Exception {
        super.setUpOnce();
        TestEntity e = new TestEntity();
        e.setName("someName");
        e = dao.save(e);
        Assertions.assertThat(e.getId()).isNotNull();
        final TestEntity findOneRandom = dao.findOneRandom();
        Assertions.assertThat(findOneRandom).isEqualTo(e);
    }

    @Test
    public void testReadNormal() {
        new TransactionalAspectMethods().testReadNormal();
    }

    @Test
    public void testReadWithDetach() {
        Assertions.assertTimeout(new Duration(30, FTimeUnit.SECONDS), new Executable() {
            @Override
            public void execute() throws Throwable {
                new TransactionalAspectMethods().testReadWithDetach();
            }
        });

    }

    @Test
    public void testReadWithSerialization() {
        new TransactionalAspectMethods().testReadWithSerialization();

    }

    @Test
    public void testNestedTransactions() {
        Assertions.assertTimeout(new Duration(60, FTimeUnit.SECONDS), new Executable() {
            @Override
            public void execute() throws Throwable {
                new TransactionalAspectMethods().testNestedTransactions();
            }
        });
    }

    @Test
    public void testBatchInsert() {
        Assertions.assertTimeout(new Duration(10, FTimeUnit.SECONDS), new Executable() {
            @Override
            public void execute() throws Throwable {
                new TransactionalAspectMethods().testBatchInsert();
            }
        });
    }

    private class TransactionalAspectMethods {

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

}
