package de.invesdwin.context.persistence.jpa.api.bulkinsert;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import org.junit.Test;

import de.invesdwin.context.persistence.jpa.complex.TestDao;
import de.invesdwin.context.persistence.jpa.complex.TestEntity;
import de.invesdwin.context.persistence.jpa.test.PersistenceTest;
import de.invesdwin.context.persistence.jpa.test.PersistenceTestContext;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
@PersistenceTest(PersistenceTestContext.SERVER)
public class BulkInsertEntitiesTest extends ATest {

    @Inject
    private TestDao testDao;

    @Test
    public void testLoadDataInfile() {
        testDao.deleteAll();
        final BulkInsertEntities<TestEntity> loadDataInfile = new BulkInsertEntities<TestEntity>(TestEntity.class);
        Assertions.assertThat(loadDataInfile.persist()).isEqualTo(0);
        Assertions.assertThat(testDao.count()).isEqualTo(0);
        final int ents = 1000;
        final List<TestEntity> list = new ArrayList<TestEntity>();
        for (int i = 0; i < ents; i++) {
            final TestEntity entity = new TestEntity();
            entity.setName(i + "");
            list.add(entity);
        }
        loadDataInfile.stage(list);
        Assertions.assertThat(loadDataInfile.persist()).isEqualTo(ents);
        Assertions.assertThat(testDao.count()).isEqualTo(ents);
        loadDataInfile.close();
    }
}
