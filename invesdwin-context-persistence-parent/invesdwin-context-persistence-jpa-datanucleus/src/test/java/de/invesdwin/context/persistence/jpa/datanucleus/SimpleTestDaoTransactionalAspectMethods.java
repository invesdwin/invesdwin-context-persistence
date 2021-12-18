package de.invesdwin.context.persistence.jpa.datanucleus;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.validation.ConstraintViolationException;

import org.assertj.core.api.Fail;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.dao.EmptyResultDataAccessException;
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

@NotThreadSafe
@Configurable
public class SimpleTestDaoTransactionalAspectMethods {

    private final Log log = new Log(this);

    @Inject
    private SimpleTestDao dao;
    @javax.persistence.PersistenceUnit(unitName = PersistenceProperties.DEFAULT_PERSISTENCE_UNIT_NAME)
    private EntityManagerFactory factory;
    @Inject
    private SimpleTestService service;

    private int transactionRetryInvocations = 0;

    public void testDeleteWithoutTransaction() {
        SimpleTestEntity ent1 = new SimpleTestEntity();
        ent1.setName("one");
        ent1 = dao.save(ent1);
        Assertions.assertThat(dao.findOneById(ent1.getId())).isNotNull();

        final SimpleTestEntity ent2 = new SimpleTestEntity();
        ent2.setName("999");
        dao.save(ent2);

        dao.delete(ent1);
        final SimpleTestEntity ent2lazy = new SimpleTestEntity();
        ent2lazy.setId(ent2.getId());
        dao.delete(ent2lazy);
        Assertions.assertThat(dao.findOneById(ent2lazy.getId())).isNull();

        Assertions.assertThat(dao.findAll().size()).isZero();

        //resurrection behaviour is quite inconsistent among ORMs
        final SimpleTestEntity ent1Resurrected1 = dao.save(ent1);
        Assertions.assertThat(ent1Resurrected1.getId()).isNotEqualTo(ent1.getId());
        final SimpleTestEntity ent1Resurrected2 = dao.save(ent1);
        Assertions.assertThat(ent1Resurrected2.getId()).isNotEqualTo(ent1.getId());
        Assertions.assertThat(ent1Resurrected1.getId()).isNotEqualTo(ent1Resurrected2.getId());
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
            SimpleTestEntity ent1 = new SimpleTestEntity();
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
            SimpleTestEntity ent = new SimpleTestEntity();
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
        final SimpleTestEntity ent = new SimpleTestEntity();
        ent.setId(Long.MAX_VALUE);
        try {
            dao.delete(ent);
            Fail.fail("Exception should have been thrown!");
        } catch (final Throwable t) {
            final boolean causedBy = Throwables.isCausedByType(t, EmptyResultDataAccessException.class);
            if (!causedBy) {
                throw Err.process(t);
            }
        }
    }

    @Transactional
    public void testDeleteByExample() {
        final String nameEven = "even";
        final String nameUneven = "uneven";
        for (int i = 0; i < 10; i++) {
            final SimpleTestEntity e = new SimpleTestEntity();
            if (i % 2 == 0) {
                e.setName(nameEven);
            } else {
                e.setName(nameUneven);
            }
            dao.save(e);
        }

        final long countBeforeEvenName = dao.count();
        final SimpleTestEntity deleteExampleEvenNames = new SimpleTestEntity();
        deleteExampleEvenNames.setName(nameEven);
        dao.deleteAll(deleteExampleEvenNames);
        Assertions.assertThat(dao.count()).isEqualTo(countBeforeEvenName - 5);

        final long countBeforeWrongName = dao.count();
        final SimpleTestEntity deleteExampleWithWrongName = new SimpleTestEntity();
        deleteExampleWithWrongName.setName("wrongName");
        dao.deleteAll(deleteExampleWithWrongName);
        Assertions.assertThat(dao.count()).isEqualTo(countBeforeWrongName);

        dao.deleteAll();
        Assertions.assertThat(dao.count()).isEqualTo(0);
    }

    @Transactional
    public void testDeleteByExampleSingle() {
        final String name = "huhu";
        final SimpleTestEntity ent = new SimpleTestEntity();
        ent.setName(name);
        dao.save(ent);
        final SimpleTestEntity example = new SimpleTestEntity();
        example.setName(name);
        final long countBefore = dao.count();
        dao.deleteAll(example);
        Assertions.assertThat(dao.count()).isEqualTo(countBefore - 1);
    }

    @Transactional
    public void testWriteAndRead() {
        final SimpleTestEntity ent1 = new SimpleTestEntity();
        ent1.setName("one");

        Assertions.assertThat(dao.isEmpty()).isTrue();
        dao.save(ent1);
        Assertions.assertThat(dao.isEmpty()).isFalse();
        ent1.setName("two");
        dao.save(ent1);

        final SimpleTestEntity ent2 = new SimpleTestEntity();
        ent2.setName("999");
        dao.save(ent2);

        List<SimpleTestEntity> all = dao.findAll();
        Assertions.assertThat(all.size()).isEqualTo(2);
        for (final SimpleTestEntity e : all) {
            if (e.getId().equals(ent1.getId())) {
                Assertions.assertThat(e.getName()).isEqualTo(ent1.getName());
            } else if (e.getId().equals(ent2.getId())) {
                Assertions.assertThat(e.getName()).isEqualTo(ent2.getName());
            } else {
                Fail.fail("Unbekannte Entity: " + e.getId());
            }
        }

        final SimpleTestEntity example = new SimpleTestEntity();
        example.setName("two");
        all = dao.findAll(example);
        Assertions.assertThat(all.size()).isEqualTo(1);
        Assertions.assertThat(all.get(0).getName()).isEqualTo("two");
    }

    public void testQbeIdException() {
        final SimpleTestEntity ent = new SimpleTestEntity();
        ent.setId(1L);
        dao.findAll(ent);
    }

    public void testQbeIdDeleteException() {
        final SimpleTestEntity ent = new SimpleTestEntity();
        ent.setId(1L);
        dao.deleteAll(ent);
    }

    public void testQbePersistentException() {
        SimpleTestEntity ent = new SimpleTestEntity();
        ent.setName("something");
        ent = dao.save(ent);
        dao.findAll(ent);
    }

    public void testBeanValidation() {
        try {
            final SimpleTestEntity entWithoutName = new SimpleTestEntity();
            dao.save(entWithoutName);
            Fail.fail("Exception expected");
        } catch (final Throwable t) {
            final ConstraintViolationException e = Throwables.getCauseByType(t, ConstraintViolationException.class);
            Assertions.assertThat(e).isNotNull();
            Err.process(e);
        }
    }

    public void testIllegalId() {
        final SimpleTestEntity ent = new SimpleTestEntity();
        ent.setId(null);
    }

    public void testTransactionalMissing() {
        final SimpleTestEntity ent = new SimpleTestEntity();
        ent.setName("stillworks");
        final SimpleTestEntity geschrieben = dao.save(ent);
        Assertions.assertThat(geschrieben.getId()).isNotNull();
    }

    public void testTransactionalNotMissingOnRead() {
        final SimpleTestEntity ent1 = new SimpleTestEntity();
        ent1.setId(1L);
        dao.findOne(ent1);
        dao.findAll();
    }

    public void testServiceTransactionalWithoutAnnotation() {
        service.saveSimpleTestEntityWithoutAnnotation();
    }

    @Transactional(propagation = Propagation.NEVER)
    public void testOptimisticLocking() {
        final SimpleTestEntity ent = service.saveSimpleTestEntity();

        SimpleTestEntity instance1 = getOptimisticLockingInstance(ent.getId());
        SimpleTestEntity instance2 = getOptimisticLockingInstance(ent.getId());

        Assertions.assertThat(instance1).isNotSameAs(instance2);

        instance1.setName("one");
        instance1 = service.save(instance1);
        instance2.setName("two");
        instance2 = service.save(instance2);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    private SimpleTestEntity getOptimisticLockingInstance(final Long id) {
        final EntityManager em = factory.createEntityManager();
        try {
            final SimpleTestEntity ent = em.find(SimpleTestEntity.class, id);
            return ent;
        } finally {
            em.close();
        }
    }

    @SuppressWarnings("unchecked")
    public void testQueryWithNullParameter() {
        final EntityManager em = factory.createEntityManager();
        final List<SimpleTestEntity> res = em
                .createQuery("SELECT e FROM " + SimpleTestEntity.class.getName() + " e WHERE name = :name")
                .setParameter("name", null)
                .getResultList();
        Assertions.assertThat(res.size()).isZero();
    }

    public void testRollback() {
        final String name = "thisCausesARollBack";

        final SimpleTestEntity ent = new SimpleTestEntity();
        ent.setName(name);
        try {
            service.saveEntityAndRollback(ent);
        } catch (final IllegalStateException e) {
            Err.process(e);
        }
        final SimpleTestEntity gelesen = service.getByName(name);
        Assertions.assertThat(gelesen).isNull();
    }

    public void testRequiresNewTransaction() {
        final String name = "thisWillBeKept";

        final SimpleTestEntity ent = new SimpleTestEntity();
        ent.setName(name);
        try {
            service.saveEntityAndRollbackAfterRequiresNewMethodCall(ent);
        } catch (final IllegalStateException e) {
            Err.process(e);
        }

        final SimpleTestEntity findOne = service.getByName(name);
        Assertions.assertThat(findOne).isNotNull();
        Assertions.assertThat(ent.getId()).isEqualTo(findOne.getId());
    }

    @Transactional
    public void testMultipleReadsInOneTransactionCausesOnlyOneSelect() {
        final String name = "someName";
        SimpleTestEntity ent = new SimpleTestEntity();
        ent.setName(name);
        ent = dao.save(ent);

        final SimpleTestEntity anderesEnt = new SimpleTestEntity();
        anderesEnt.setName("anotherName");
        dao.save(anderesEnt);
        dao.flush();

        SimpleTestEntity newEnt1 = new SimpleTestEntity();
        newEnt1.setId(ent.getId());
        newEnt1 = dao.findOne(newEnt1);

        SimpleTestEntity newEnt2 = new SimpleTestEntity();
        newEnt2.setId(ent.getId());
        newEnt2 = dao.findOne(newEnt2);

        Assertions.assertThat(newEnt1).isEqualTo(newEnt2);
        Assertions.assertThat(newEnt1).isSameAs(newEnt2);

        SimpleTestEntity cloneEnt1 = newEnt2.clone();
        cloneEnt1 = dao.findOne(cloneEnt1);
        Assertions.assertThat(newEnt2).isEqualTo(cloneEnt1);
        Assertions.assertThat(newEnt2).isSameAs(cloneEnt1);

        SimpleTestEntity cloneEnt2 = newEnt2.clone();
        cloneEnt2 = dao.findOne(cloneEnt2);
        Assertions.assertThat(newEnt2).isEqualTo(cloneEnt2);
        Assertions.assertThat(newEnt2).isSameAs(cloneEnt2);
    }

    public void testMultipleReadsInNewTransactionsCausesNewSelect() {
        testMultipleReadsInOneTransactionCausesOnlyOneSelect();

        final SimpleTestEntity newEnt1 = dao.findAll(new QueryConfig().setMaxResults(1)).get(0);

        SimpleTestEntity newEnt2 = new SimpleTestEntity();
        newEnt2.setId(newEnt1.getId());
        newEnt2 = dao.findOne(newEnt2);

        Assertions.assertThat(newEnt1).isEqualTo(newEnt2);
        Assertions.assertThat(newEnt1).isNotSameAs(newEnt2);
    }

    public void testMultipleMergeInNewTransactionsDoesNotCreateInsert() {
        testMultipleReadsInOneTransactionCausesOnlyOneSelect();

        final long countLinesBefore = dao.count();
        final SimpleTestEntity newEnt1 = dao.findAll(new QueryConfig().setMaxResults(1)).get(0);

        newEnt1.setName("newName");
        final SimpleTestEntity updatedEnt1 = dao.save(newEnt1);

        Assertions.assertThat(updatedEnt1).isEqualTo(newEnt1);
        Assertions.assertThat(updatedEnt1.getId()).isEqualTo(newEnt1.getId());
        Assertions.assertThat(updatedEnt1).isNotSameAs(newEnt1);
        Assertions.assertThat(dao.count()).isEqualTo(countLinesBefore);
    }

    public void testMultipleReadOfSameObjectCausesChangesToBeReset() {
        testMultipleReadsInOneTransactionCausesOnlyOneSelect();

        SimpleTestEntity newEnt1 = dao.findAll(new QueryConfig().setMaxResults(1)).get(0);
        final String alterName = newEnt1.getName();
        newEnt1.setName("newName");
        newEnt1 = dao.findOne(newEnt1);
        Assertions.assertThat(newEnt1.getName()).isEqualTo(alterName);
    }

    public void testMagicalUpdateInvokedWithoutCallingWrite() {
        final String newName = "newName";
        //Magical Update does not get invoked when serializing before manipulation
        SimpleTestEntity manipuliertesEnt = testMagicalUpdateWithoutWriteCallInnerTransactionWithManipulationNoWrite(
                newName, true);
        SimpleTestEntity newEnt1 = new SimpleTestEntity();
        newEnt1.setId(manipuliertesEnt.getId());
        newEnt1 = dao.findOne(newEnt1);
        Assertions.assertThat(newEnt1.getName()).isNotEqualTo(manipuliertesEnt.getName());

        //Magical update gets calles after transaction without using serialization before manipulation
        manipuliertesEnt = testMagicalUpdateWithoutWriteCallInnerTransactionWithManipulationNoWrite(newName, false);
        newEnt1 = new SimpleTestEntity();
        newEnt1.setId(manipuliertesEnt.getId());
        newEnt1 = dao.findOne(newEnt1);
        Assertions.assertThat(newEnt1.getName()).isEqualTo(manipuliertesEnt.getName());
    }

    @Transactional
    private SimpleTestEntity testMagicalUpdateWithoutWriteCallInnerTransactionWithManipulationNoWrite(
            final String newName, final boolean withCloneBeforeManipulation) {
        testMultipleReadsInOneTransactionCausesOnlyOneSelect();
        SimpleTestEntity newEnt = dao.findAll(new QueryConfig().setMaxResults(1)).get(0);
        if (withCloneBeforeManipulation) {
            newEnt = newEnt.clone();
        }
        newEnt.setName(newName);
        return newEnt;
    }

    public void testUnicode() {
        final String unicode = "æøå";
        SimpleTestEntity ent = new SimpleTestEntity();
        ent.setName(unicode);
        ent = dao.save(ent);
        Assertions.assertThat(ent.getName()).isEqualTo(unicode);

        final SimpleTestEntity example = new SimpleTestEntity();
        example.setId(ent.getId());
        final SimpleTestEntity entGelesen = dao.findOne(example);
        log.info("%s=%s", unicode, entGelesen.getName());
        Assertions.assertThat(entGelesen.getName()).isEqualTo(unicode);
        Assertions.assertThat(entGelesen.getName()).isEqualTo(ent.getName());
    }

    @SuppressWarnings("unchecked")
    public void testQueryDslJpa() {
        for (int i = 0; i < 5; i++) {
            final SimpleTestEntity vo = new SimpleTestEntity();
            vo.setName("" + i);
            dao.save(vo);
        }
        final SimpleTestEntity vo = Alias.alias(SimpleTestEntity.class);
        Assertions.assertThat(vo).isNotNull();
        final JPAQuery<SimpleTestEntity> query = new JPAQuery<SimpleTestEntity>(dao.getEntityManager());
        final EntityPath<SimpleTestEntity> fromVo = (EntityPath<SimpleTestEntity>) Alias.$(vo);
        Assertions.assertThat(fromVo).as("https://bugs.launchpad.net/querydsl/+bug/785935").isNotNull();
        query.from(fromVo);
        query.where(Alias.$(vo.getName()).eq("1"));
        final List<SimpleTestEntity> result = query.select(Alias.$(vo)).fetch();

        Assertions.assertThat(result).isNotNull();
        Assertions.assertThat(result.size()).isEqualTo(1);
        Assertions.assertThat(result.get(0).getName()).isEqualTo("1");

        query.where(Alias.$(vo.getName()).eq("2"));
        final List<SimpleTestEntity> resultEmpty = query.select(Alias.$(vo)).fetch();
        Assertions.assertThat(resultEmpty.size()).isZero();
    }

}