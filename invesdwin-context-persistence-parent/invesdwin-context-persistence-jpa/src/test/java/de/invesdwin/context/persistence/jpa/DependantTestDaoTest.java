package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.ThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.persistence.jpa.complex.TestDao;
import de.invesdwin.context.persistence.jpa.complex.TestEntity;
import de.invesdwin.context.persistence.jpa.test.APersistenceTest;
import de.invesdwin.util.assertions.Assertions;
import jakarta.inject.Inject;

@ThreadSafe
public class DependantTestDaoTest extends APersistenceTest {

    @Inject
    private DependantTestDao dependantTestDao;
    @Inject
    private TestDao testDao;

    /**
     * CascadeDelete is quite useless in this form...
     */
    @Test
    public void testCascadeDelete() {
        TestEntity testEntity = new TestEntity();
        testEntity.setName("testCascadeDelete");
        testEntity = testDao.save(testEntity);

        final DependantTestEntity dependantTestEntity = new DependantTestEntity();
        dependantTestEntity.setName("testCascadeDeleteDependant");
        dependantTestEntity.setVater(testEntity);
        dependantTestDao.save(dependantTestEntity);

        dependantTestDao.delete(dependantTestEntity);

        final TestEntity example = new TestEntity();
        example.setId(testEntity.getId());
        Assertions.assertThat(testDao.findOne(example)).isNull();
        final DependantTestEntity exampleDependant = new DependantTestEntity();
        exampleDependant.setId(dependantTestEntity.getId());
        Assertions.assertThat(dependantTestDao.findOne(exampleDependant)).isNull();
    }

}
