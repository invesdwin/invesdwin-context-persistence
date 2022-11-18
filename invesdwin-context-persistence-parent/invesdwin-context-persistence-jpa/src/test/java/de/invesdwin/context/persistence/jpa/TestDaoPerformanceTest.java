package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.ThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.persistence.jpa.complex.TestDao;
import de.invesdwin.context.persistence.jpa.complex.TestEntity;
import de.invesdwin.context.persistence.jpa.test.APersistenceTest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.assertions.Executable;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import jakarta.inject.Inject;

@ThreadSafe
//@ContextConfiguration(locations = { APersistenzTest.CTX_TEST_SERVER }, inheritLocations = false)
public class TestDaoPerformanceTest extends APersistenceTest {

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
        new TestDaoPerformanceTransactionalAspectMethods().testReadNormal();
    }

    @Test
    public void testReadWithDetach() {
        Assertions.assertTimeout(new Duration(30, FTimeUnit.SECONDS), new Executable() {
            @Override
            public void execute() throws Throwable {
                new TestDaoPerformanceTransactionalAspectMethods().testReadWithDetach();
            }
        });

    }

    @Test
    public void testReadWithSerialization() {
        new TestDaoPerformanceTransactionalAspectMethods().testReadWithSerialization();

    }

    @Test
    public void testNestedTransactions() {
        Assertions.assertTimeout(new Duration(60, FTimeUnit.SECONDS), new Executable() {
            @Override
            public void execute() throws Throwable {
                new TestDaoPerformanceTransactionalAspectMethods().testNestedTransactions();
            }
        });
    }

    @Test
    public void testBatchInsert() {
        Assertions.assertTimeout(new Duration(10, FTimeUnit.SECONDS), new Executable() {
            @Override
            public void execute() throws Throwable {
                new TestDaoPerformanceTransactionalAspectMethods().testBatchInsert();
            }
        });
    }

}
