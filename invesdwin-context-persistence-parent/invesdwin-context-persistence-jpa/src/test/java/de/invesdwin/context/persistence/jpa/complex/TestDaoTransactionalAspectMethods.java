package de.invesdwin.context.persistence.jpa.complex;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.assertj.core.api.Fail;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.querydsl.core.alias.Alias;
import com.querydsl.core.types.EntityPath;
import com.querydsl.jpa.impl.JPAQuery;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.api.query.QueryConfig;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.Throwables;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.validation.ConstraintViolationException;

@Configurable
@NotThreadSafe
public class TestDaoTransactionalAspectMethods {

    private final Log log = new Log(this);

    @Inject
    private TestDao dao;
    @jakarta.persistence.PersistenceUnit(unitName = PersistenceProperties.DEFAULT_PERSISTENCE_UNIT_NAME)
    private EntityManagerFactory factory;
    @Inject
    private TestService service;

    //CHECKSTYLE:OFF
    public int transactionRetryInvocations = 0;
    //CHECKSTYLE:ON

    public void testDeleteWithoutTransaction() {
        TestEntity ent1 = new TestEntity();
        ent1.setName("one");
        ent1 = dao.save(ent1);
        Assertions.assertThat(dao.findOneById(ent1.getId())).isNotNull();

        final TestEntity ent2 = new TestEntity();
        ent2.setName("999");
        dao.save(ent2);

        dao.delete(ent1);
        final TestEntity ent2lazy = new TestEntity();
        ent2lazy.setId(ent2.getId());
        dao.delete(ent2lazy);
        Assertions.assertThat(dao.findOneById(ent2lazy.getId())).isNull();

        Assertions.assertThat(dao.findAll().size()).isZero();

        //resurrection behaviour is quite inconsistent among ORMs (causes OptimisticLockException in Hibernate 6.6.1.Final)
        //        final TestEntity ent1Resurrected1 = dao.save(ent1);
        //        Assertions.assertThat(ent1Resurrected1.getId()).isNotEqualTo(ent1.getId());
        //        final TestEntity ent1Resurrected2 = dao.save(ent1);
        //        Assertions.assertThat(ent1Resurrected2.getId()).isNotEqualTo(ent1.getId());
        //        Assertions.assertThat(ent1Resurrected1.getId()).isNotEqualTo(ent1Resurrected2.getId());
    }

    public void testTransactionRetry() {
        dao.deleteAll();
        Assertions.assertThat(transactionRetryInvocations).isEqualTo(0);
        testTransactionRetryInner();
        Assertions.assertThat(transactionRetryInvocations).isEqualTo(2);
        Assertions.assertThat(dao.count()).isEqualTo(1);
    }

    @Transactional
    private void testTransactionRetryInner() {
        try {
            TestEntity ent1 = new TestEntity();
            ent1.setName("testTransactionRetry");
            ent1 = dao.save(ent1);
            if (transactionRetryInvocations == 0) {
                throw new TransientDataAccessException("bla bla") {

                    private static final long serialVersionUID = 1L;
                };
            }
        } finally {
            transactionRetryInvocations++;
        }
    }

    @Transactional
    public void testDeleteByIds() {
        final List<Long> ids = new ArrayList<Long>();
        for (int i = 0; i < 10; i++) {
            TestEntity ent = new TestEntity();
            ent.setName("" + i);
            ent = dao.save(ent);
            Assertions.assertThat(dao.findOneById(ent.getId())).isNotNull();
            ids.add(ent.getId());
        }
        Assertions.assertThat(dao.findAll()).hasSize(ids.size());
        dao.deleteIds(new ArrayList<Long>());
        Assertions.assertThat(dao.findAll()).hasSize(ids.size());
        dao.deleteIds(ids.subList(0, ids.size() / 2));
        Assertions.assertThat(dao.findAll()).hasSize(ids.size() / 2);
        dao.deleteIds(ids);
        Assertions.assertThat(dao.findAll()).hasSize(0);
    }

    @Transactional
    public void testDeleteWithTransaction() {
        testDeleteWithoutTransaction();
    }

    public void testFlushWithoutTransaction() {
        dao.flush();
    }

    public void testDeleteNonExisting() {
        final TestEntity ent = new TestEntity();
        ent.setId(Long.MAX_VALUE);
        dao.delete(ent);
    }

    @Transactional
    public void testDeleteByExample() {
        final String nameEven = "even";
        final String nameUneven = "uneven";
        for (int i = 0; i < 10; i++) {
            final TestEntity e = new TestEntity();
            if (i % 2 == 0) {
                e.setName(nameEven);
            } else {
                e.setName(nameUneven);
            }
            dao.save(e);
        }

        final long countBeforeEvenName = dao.count();
        final TestEntity deleteExampleEvenNames = new TestEntity();
        deleteExampleEvenNames.setName(nameEven);
        dao.deleteAll(deleteExampleEvenNames);
        Assertions.assertThat(dao.count()).isEqualTo(countBeforeEvenName - 5);

        final long countBeforeWrongName = dao.count();
        final TestEntity deleteExampleWithWrongName = new TestEntity();
        deleteExampleWithWrongName.setName("wrongName");
        dao.deleteAll(deleteExampleWithWrongName);
        Assertions.assertThat(dao.count()).isEqualTo(countBeforeWrongName);

        dao.deleteAll();
        Assertions.assertThat(dao.count()).isEqualTo(0);
    }

    @Transactional
    public void testDeleteByExampleSingle() {
        final String name = "huhu";
        final TestEntity ent = new TestEntity();
        ent.setName(name);
        dao.save(ent);
        final TestEntity example = new TestEntity();
        example.setName(name);
        final long countBefore = dao.count();
        dao.deleteAll(example);
        Assertions.assertThat(dao.count()).isEqualTo(countBefore - 1);
    }

    @Transactional
    public void testWriteAndRead() {
        final TestEntity ent1 = new TestEntity();
        ent1.setName("one");

        Assertions.assertThat(dao.isEmpty()).isTrue();
        dao.save(ent1);
        Assertions.assertThat(dao.isEmpty()).isFalse();
        Assertions.assertThat(ent1.getCreated()).isNotNull();
        Assertions.assertThat(ent1.getUpdated()).isNotNull();
        ent1.setName("two");
        dao.save(ent1);

        final TestEntity ent2 = new TestEntity();
        ent2.setName("999");
        dao.save(ent2);

        List<TestEntity> all = dao.findAll();
        Assertions.assertThat(all.size()).isEqualTo(2);
        for (final TestEntity e : all) {
            if (e.getId().equals(ent1.getId())) {
                Assertions.assertThat(e.getName()).isEqualTo(ent1.getName());
            } else if (e.getId().equals(ent2.getId())) {
                Assertions.assertThat(e.getName()).isEqualTo(ent2.getName());
            } else {
                Fail.fail("Unbekannte Entity: " + e.getId());
            }
        }

        final TestEntity example = new TestEntity();
        example.setName("two");
        all = dao.findAll(example);
        Assertions.assertThat(all.size()).isEqualTo(1);
        Assertions.assertThat(all.get(0).getName()).isEqualTo("two");
    }

    public void testQbeIdException() {
        final TestEntity ent = new TestEntity();
        ent.setId(1L);
        dao.findAll(ent);
    }

    public void testQbeIdDeleteException() {
        final TestEntity ent = new TestEntity();
        ent.setId(1L);
        dao.deleteAll(ent);
    }

    public void testQbePersistentException() {
        TestEntity ent = new TestEntity();
        ent.setName("something");
        ent = dao.save(ent);
        dao.findAll(ent);
    }

    public void testBeanValidation() {
        try {
            final TestEntity entWithoutName = new TestEntity();
            dao.save(entWithoutName);
            Fail.fail("Exception expected");
        } catch (final Throwable t) {
            final ConstraintViolationException e = Throwables.getCauseByType(t, ConstraintViolationException.class);
            Assertions.assertThat(e).isNotNull();
            Err.process(e);
        }
    }

    public void testIllegalId() {
        final TestEntity ent = new TestEntity();
        ent.setId(null);
    }

    public void testTransactionalMissing() {
        final TestEntity ent = new TestEntity();
        ent.setName("stillworks");
        final TestEntity geschrieben = dao.save(ent);
        Assertions.assertThat(geschrieben.getId()).isNotNull();
    }

    public void testTransactionalNotMissingOnRead() {
        final TestEntity ent1 = new TestEntity();
        ent1.setId(1L);
        dao.findOne(ent1);
        dao.findAll();
    }

    public void testServiceTransactionalWithoutAnnotation() {
        service.saveTestEntityWithoutAnnotation();
    }

    @Transactional(propagation = Propagation.NEVER)
    public void testOptimisticLocking() {
        final TestEntity ent = service.saveTestEntity();

        TestEntity instance1 = getOptimisticLockingInstance(ent.getId());
        TestEntity instance2 = getOptimisticLockingInstance(ent.getId());

        Assertions.assertThat(instance1).isNotSameAs(instance2);

        instance1.setName("one");
        instance1 = service.save(instance1);
        instance2.setName("two");
        instance2 = service.save(instance2);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    private TestEntity getOptimisticLockingInstance(final Long id) {
        final EntityManager em = factory.createEntityManager();
        try {
            final TestEntity ent = em.find(TestEntity.class, id);
            return ent;
        } finally {
            em.close();
        }
    }

    @SuppressWarnings("unchecked")
    public void testQueryWithNullParameter() {
        final EntityManager em = factory.createEntityManager();
        final List<TestEntity> res = em
                .createQuery("SELECT e FROM " + TestEntity.class.getName() + " e WHERE e.name = :name")
                .setParameter("name", null)
                .getResultList();
        Assertions.assertThat(res.size()).isZero();
    }

    public void testRollback() {
        final String name = "thisCausesARollBack";

        final TestEntity ent = new TestEntity();
        ent.setName(name);
        try {
            service.saveEntityAndRollback(ent);
        } catch (final IllegalStateException e) {
            Err.process(e);
        }
        final TestEntity gelesen = service.getByName(name);
        Assertions.assertThat(gelesen).isNull();
    }

    public void testRequiresNewTransaction() {
        final String name = "thisWillBeKept";

        final TestEntity ent = new TestEntity();
        ent.setName(name);
        try {
            service.saveEntityAndRollbackAfterRequiresNewMethodCall(ent);
        } catch (final IllegalStateException e) {
            Err.process(e);
        }

        final TestEntity findOne = service.getByName(name);
        Assertions.assertThat(findOne).isNotNull();
        Assertions.assertThat(ent.getId()).isEqualTo(findOne.getId());
    }

    @Transactional
    public void testMultipleReadsInOneTransactionCausesOnlyOneSelect() {
        final String name = "someName";
        TestEntity ent = new TestEntity();
        ent.setName(name);
        ent = dao.save(ent);

        final TestEntity anderesEnt = new TestEntity();
        anderesEnt.setName("anotherName");
        dao.save(anderesEnt);
        dao.flush();

        TestEntity newEnt1 = new TestEntity();
        newEnt1.setId(ent.getId());
        newEnt1 = dao.findOne(newEnt1);

        TestEntity newEnt2 = new TestEntity();
        newEnt2.setId(ent.getId());
        newEnt2 = dao.findOne(newEnt2);

        Assertions.assertThat(newEnt1).isEqualTo(newEnt2);
        Assertions.assertThat(newEnt1).isSameAs(newEnt2);

        TestEntity cloneEnt1 = (TestEntity) newEnt2.clone();
        cloneEnt1 = dao.findOne(cloneEnt1);
        Assertions.assertThat(newEnt2).isEqualTo(cloneEnt1);
        Assertions.assertThat(newEnt2).isSameAs(cloneEnt1);

        TestEntity cloneEnt2 = (TestEntity) newEnt2.clone();
        cloneEnt2 = dao.findOne(cloneEnt2);
        Assertions.assertThat(newEnt2).isEqualTo(cloneEnt2);
        Assertions.assertThat(newEnt2).isSameAs(cloneEnt2);
    }

    public void testMultipleReadsInNewTransactionsCausesNewSelect() {
        testMultipleReadsInOneTransactionCausesOnlyOneSelect();

        final TestEntity newEnt1 = dao.findAll(new QueryConfig().setMaxResults(1)).get(0);

        TestEntity newEnt2 = new TestEntity();
        newEnt2.setId(newEnt1.getId());
        newEnt2 = dao.findOne(newEnt2);

        Assertions.assertThat(newEnt1).isEqualTo(newEnt2);
        Assertions.assertThat(newEnt1).isNotSameAs(newEnt2);
    }

    public void testMultipleMergeInNewTransactionsDoesNotCreateInsert() {
        testMultipleReadsInOneTransactionCausesOnlyOneSelect();

        final long countLinesBefore = dao.count();
        final TestEntity newEnt1 = dao.findAll(new QueryConfig().setMaxResults(1)).get(0);

        newEnt1.setName("newName");
        final TestEntity updatedEnt1 = dao.save(newEnt1);

        Assertions.assertThat(updatedEnt1).isEqualTo(newEnt1);
        Assertions.assertThat(updatedEnt1.getVersion()).isNotEqualTo(newEnt1.getVersion());
        Assertions.assertThat(updatedEnt1.getId()).isEqualTo(newEnt1.getId());
        Assertions.assertThat(updatedEnt1).isNotSameAs(newEnt1);
        Assertions.assertThat(dao.count()).isEqualTo(countLinesBefore);
    }

    public void testMultipleReadOfSameObjectCausesChangesToBeReset() {
        testMultipleReadsInOneTransactionCausesOnlyOneSelect();

        TestEntity newEnt1 = dao.findAll(new QueryConfig().setMaxResults(1)).get(0);
        final String alterName = newEnt1.getName();
        newEnt1.setName("newName");
        newEnt1 = dao.findOne(newEnt1);
        Assertions.assertThat(newEnt1.getName()).isEqualTo(alterName);
    }

    public void testMagicalUpdateInvokedWithoutCallingWrite() {
        final String newName = "newName";
        //Magical Update does not get invoked when serializing before manipulation
        TestEntity manipuliertesEnt = testMagicalUpdateWithoutWriteCallInnerTransactionWithManipulationNoWrite(newName,
                true);
        TestEntity newEnt1 = new TestEntity();
        newEnt1.setId(manipuliertesEnt.getId());
        newEnt1 = dao.findOne(newEnt1);
        Assertions.assertThat(newEnt1.getName()).isNotEqualTo(manipuliertesEnt.getName());

        //Magical update gets calles after transaction without using serialization before manipulation
        manipuliertesEnt = testMagicalUpdateWithoutWriteCallInnerTransactionWithManipulationNoWrite(newName, false);
        newEnt1 = new TestEntity();
        newEnt1.setId(manipuliertesEnt.getId());
        newEnt1 = dao.findOne(newEnt1);
        Assertions.assertThat(newEnt1.getName()).isEqualTo(manipuliertesEnt.getName());
    }

    @Transactional
    private TestEntity testMagicalUpdateWithoutWriteCallInnerTransactionWithManipulationNoWrite(final String newName,
            final boolean withCloneBeforeManipulation) {
        testMultipleReadsInOneTransactionCausesOnlyOneSelect();
        TestEntity newEnt = dao.findAll(new QueryConfig().setMaxResults(1)).get(0);
        if (withCloneBeforeManipulation) {
            newEnt = (TestEntity) newEnt.clone();
        }
        newEnt.setName(newName);
        return newEnt;
    }

    public void testMergeFrom() {
        final String name = "asdf";
        final TestVO vo = new TestVO();
        vo.setName(name);
        final TestEntity e1 = new TestEntity();
        e1.mergeFrom(vo);
        Assertions.assertThat(e1.getName()).isEqualTo(name);
        final TestEntity e2 = new TestEntity();
        e2.mergeFrom(e1);
        Assertions.assertThat(e2.getName()).isEqualTo(name);
    }

    public void testUnicode() {
        final String unicode = "æøå";
        TestEntity ent = new TestEntity();
        ent.setName(unicode);
        ent = dao.save(ent);
        Assertions.assertThat(ent.getName()).isEqualTo(unicode);

        final TestEntity example = new TestEntity();
        example.setId(ent.getId());
        final TestEntity entGelesen = dao.findOne(example);
        log.info("%s=%s", unicode, entGelesen.getName());
        Assertions.assertThat(entGelesen.getName()).isEqualTo(unicode);
        Assertions.assertThat(entGelesen.getName()).isEqualTo(ent.getName());
    }

    @SuppressWarnings("unchecked")
    public void testQueryDslJpa() {
        for (int i = 0; i < 5; i++) {
            final TestEntity vo = new TestEntity();
            vo.setName("" + i);
            dao.save(vo);
        }
        final TestEntity vo = Alias.alias(TestEntity.class);
        Assertions.assertThat(vo).isNotNull();
        final JPAQuery<TestEntity> query = new JPAQuery<TestEntity>(dao.getEntityManager());
        final EntityPath<TestEntity> fromVo = (EntityPath<TestEntity>) Alias.$(vo);
        Assertions.assertThat(fromVo).as("https://bugs.launchpad.net/querydsl/+bug/785935").isNotNull();
        query.from(fromVo);
        query.where(Alias.$(vo.getName()).eq("1"));
        final List<TestEntity> result = query.select(Alias.$(vo)).fetch();

        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.size()).isEqualTo(1);
        Assertions.assertThat(result.get(0).getName()).isEqualTo("1");

        query.where(Alias.$(vo.getName()).eq("2"));
        final List<TestEntity> resultEmpty = query.select(Alias.$(vo)).fetch();
        Assertions.assertThat(resultEmpty.size()).isZero();
    }

}