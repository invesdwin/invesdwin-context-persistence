package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import org.junit.Test;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.persistence.jpa.api.query.QueryConfig;
import de.invesdwin.context.persistence.jpa.complex.TestDao;
import de.invesdwin.context.persistence.jpa.complex.TestEntity;
import de.invesdwin.context.persistence.jpa.test.APersistenceTest;
import de.invesdwin.util.assertions.Assertions;

@ThreadSafe
@Transactional(value = PersistenceProperties.DEFAULT_TRANSACTION_MANAGER_NAME, propagation = Propagation.NEVER)
public class CachedTestDaoTest extends APersistenceTest {

    @Inject
    private TestDao testDao;
    @Inject
    private CachedTestDao cachedTestDao;

    @Test(timeout = 1000)
    public void testNoL2Cache() {
        new TransactionalAspectMethods().testNoL2Cache();
    }

    @Test(timeout = 1000)
    public void testNoL2CacheWithQueryCache() {
        new TransactionalAspectMethods().testNoL2CacheWithQueryCache();
    }

    @Test(timeout = 1000)
    public void testL2Cache() {
        new TransactionalAspectMethods().testL2Cache();
    }

    @Test(timeout = 1000)
    public void testL2CacheWithoutQueryCache() {
        new TransactionalAspectMethods().testL2CacheWithoutQueryCache();
    }

    @Test(timeout = 1000)
    public void testL2CacheWithQueryCache() {
        new TransactionalAspectMethods().testL2CacheWithQueryCache();
    }

    private class TransactionalAspectMethods {

        @Transactional(propagation = Propagation.NEVER)
        public void testNoL2Cache() {
            TestEntity ent = new TestEntity();
            ent.setName("testNoL2Cache");
            ent = testDao.save(ent);

            for (int i = 0; i < 100; i++) {
                final TestEntity qbe = new TestEntity();
                qbe.setId(ent.getId());
                Assertions.assertThat(testDao.findOne(qbe)).isNotSameAs(ent);
            }
        }

        @Transactional(propagation = Propagation.NEVER)
        public void testNoL2CacheWithQueryCache() {
            TestEntity ent = new TestEntity();
            ent.setName("testNoL2CacheWithQueryCache");
            ent = testDao.save(ent);

            for (int i = 0; i < 100; i++) {
                final TestEntity qbe = new TestEntity();
                qbe.setName(ent.getName());
                Assertions.assertThat(testDao.findOne(qbe, new QueryConfig().withCacheable(true))).isNotSameAs(ent);
            }
        }

        @Transactional(propagation = Propagation.NEVER)
        public void testL2Cache() {
            CachedTestEntity ent = new CachedTestEntity();
            ent.setName("testL2Cache");
            ent = cachedTestDao.save(ent);

            for (int i = 0; i < 100; i++) {
                final CachedTestEntity qbe = new CachedTestEntity();
                qbe.setId(ent.getId());
                Assertions.assertThat(cachedTestDao.findOne(qbe)).isNotSameAs(ent);
            }
        }

        @Transactional(propagation = Propagation.NEVER)
        public void testL2CacheWithoutQueryCache() {
            CachedTestEntity ent = new CachedTestEntity();
            ent.setName("testL2CacheWithoutQueryCache");
            ent = cachedTestDao.save(ent);

            for (int i = 0; i < 100; i++) {
                final CachedTestEntity qbe = new CachedTestEntity();
                qbe.setName(ent.getName());
                Assertions.assertThat(cachedTestDao.findOne(qbe)).isNotSameAs(ent);
            }
        }

        @Transactional(propagation = Propagation.NEVER)
        public void testL2CacheWithQueryCache() {
            CachedTestEntity ent = new CachedTestEntity();
            ent.setName("testL2CacheWithQueryCache");
            ent = cachedTestDao.save(ent);

            for (int i = 0; i < 100; i++) {
                final CachedTestEntity qbe = new CachedTestEntity();
                qbe.setName(ent.getName());
                Assertions.assertThat(cachedTestDao.findOne(qbe, new QueryConfig().withCacheable(true)))
                        .isNotSameAs(ent);
            }
        }
    }

}
