package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import org.junit.Test;

import de.invesdwin.context.persistence.jpa.complex.TestEntity;
import de.invesdwin.context.persistence.jpa.test.APersistenceTest;
import de.invesdwin.util.assertions.Assertions;

@ThreadSafe
public class TestRepositoryTest extends APersistenceTest {

    @Inject
    private ITestRepository testRepo;

    @SuppressWarnings("JUnit4SetUpNotRun")
    @Override
    public void setUp() throws Exception {
        super.setUp();
        clearAllTables();
    }

    @Test
    public void testRepository() {
        Assertions.assertThat(testRepo.count()).isEqualTo(0);
        TestEntity e = new TestEntity();
        e.setName("one");
        e = testRepo.save(e);

        final TestEntity queryResult = testRepo.findTestByName(e.getName());
        Assertions.assertThat(queryResult.getId()).isEqualTo(e.getId());
        Assertions.assertThat(queryResult.getName()).isEqualTo(e.getName());

        Assertions.assertThat(testRepo.count()).isEqualTo(1);
        testRepo.delete(e);
        Assertions.assertThat(testRepo.count()).isEqualTo(0);
    }
}
