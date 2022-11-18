package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.persistence.jpa.api.query.QueryConfig;
import de.invesdwin.context.persistence.jpa.complex.TestDao;
import de.invesdwin.context.persistence.jpa.complex.TestEntity;
import de.invesdwin.util.assertions.Assertions;
import jakarta.inject.Inject;

@NotThreadSafe
@Configurable
public class CachedTestDaoTransactionalAspectMethods {

    @Inject
    private TestDao testDao;
    @Inject
    private CachedTestDao cachedTestDao;

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
            Assertions.assertThat(testDao.findOne(qbe, new QueryConfig().setCacheable(true))).isNotSameAs(ent);
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
            Assertions.assertThat(cachedTestDao.findOne(qbe, new QueryConfig().setCacheable(true))).isNotSameAs(ent);
        }
    }
}