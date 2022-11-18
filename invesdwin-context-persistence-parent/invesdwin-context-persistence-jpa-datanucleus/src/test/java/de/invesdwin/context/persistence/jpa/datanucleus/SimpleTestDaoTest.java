package de.invesdwin.context.persistence.jpa.datanucleus;

import javax.annotation.concurrent.ThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.TransactionSystemException;

import de.invesdwin.context.persistence.jpa.test.APersistenceTest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.Throwables;
import jakarta.persistence.RollbackException;

@ThreadSafe
//@ContextConfiguration(locations = { APersistenzTest.CTX_TEST_SERVER }, inheritLocations = false)
@Disabled("https://github.com/spring-projects/spring-data-jpa/issues/2357")
public class SimpleTestDaoTest extends APersistenceTest {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clearAllTables();
    }

    @Test
    @Disabled("handling without transaction is different in datanucleus compared to hibernate")
    public void testDeleteWithoutTransaction() {
        new SimpleTestDaoTransactionalAspectMethods().testDeleteWithoutTransaction();
    }

    @Test
    public void testFlushWithoutTransaction() {
        try {
            new SimpleTestDaoTransactionalAspectMethods().testFlushWithoutTransaction();
            Assertions.failExceptionExpected();
        } catch (final Throwable t) {
            Assertions.assertThat(t).isInstanceOf(IllegalTransactionStateException.class);
        }
    }

    @Test
    @Disabled("duplicate deletion throws exception in comparison to hibernate")
    public void testDeleteWithTransaction() {
        try {
            new SimpleTestDaoTransactionalAspectMethods().testDeleteWithTransaction();
        } catch (final TransactionSystemException e) {
            Assertions.assertThat(Throwables.isCausedByType(e, RollbackException.class)).isTrue();
        }
    }

    @Test
    public void testDeleteNonExisting() {
        new SimpleTestDaoTransactionalAspectMethods().testDeleteNonExisting();
    }

    @Test
    public void testDeleteByExample() {
        new SimpleTestDaoTransactionalAspectMethods().testDeleteByExample();
    }

    @Test
    public void testDeleteByIds() {
        new SimpleTestDaoTransactionalAspectMethods().testDeleteByIds();
    }

    @Test
    public void testDeleteByExampleSingle() {
        new SimpleTestDaoTransactionalAspectMethods().testDeleteByExampleSingle();
    }

    @Test
    public void testWriteAndRead() {
        new SimpleTestDaoTransactionalAspectMethods().testWriteAndRead();
    }

    @Test
    public void testQbeIdException() {
        try {
            new SimpleTestDaoTransactionalAspectMethods().testQbeIdException();
            Assertions.failExceptionExpected();
        } catch (final Throwable t) {
            Assertions.assertThat(t).isInstanceOf(AssertionError.class);
        }
    }

    @Test
    public void testQbeIdDeleteException() {
        try {
            new SimpleTestDaoTransactionalAspectMethods().testQbeIdDeleteException();
            Assertions.failExceptionExpected();
        } catch (final Throwable t) {
            Assertions.assertThat(t).isInstanceOf(AssertionError.class);
        }
    }

    @Test
    public void testQbePersistentException() {
        try {
            new SimpleTestDaoTransactionalAspectMethods().testQbePersistentException();
            Assertions.failExceptionExpected();
        } catch (final Throwable t) {
            Assertions.assertThat(t).isInstanceOf(AssertionError.class);
        }
    }

    @Test
    public void testBeanValidation() {
        new SimpleTestDaoTransactionalAspectMethods().testBeanValidation();
    }

    @Test
    public void testIllegalId() {
        try {
            new SimpleTestDaoTransactionalAspectMethods().testIllegalId();
            Assertions.failExceptionExpected();
        } catch (final Throwable t) {
            Assertions.assertThat(t).isInstanceOf(AssertionError.class);
        }
    }

    @Test
    public void testTransactionalMissing() {
        new SimpleTestDaoTransactionalAspectMethods().testTransactionalMissing();
    }

    @Test
    public void testTransactionalNotMissingOnRead() {
        new SimpleTestDaoTransactionalAspectMethods().testTransactionalNotMissingOnRead();
    }

    @Test
    public void testServiceTransactionalWithoutAnnotation() {
        new SimpleTestDaoTransactionalAspectMethods().testServiceTransactionalWithoutAnnotation();
    }

    @Test
    public void testOptimisticLocking() {
        //simple entity has no @Version, so no optimistic locking exception here
        new SimpleTestDaoTransactionalAspectMethods().testOptimisticLocking();
    }

    @Test
    public void testQueryWithNullParameter() {
        new SimpleTestDaoTransactionalAspectMethods().testQueryWithNullParameter();
    }

    @Test
    public void testRollback() {
        new SimpleTestDaoTransactionalAspectMethods().testRollback();
    }

    @Test
    public void testRequiresNewTransaction() {
        new SimpleTestDaoTransactionalAspectMethods().testRequiresNewTransaction();
    }

    @Test
    public void testMultipleReadsInOneTransactionCausesOnlyOneSelect() {
        new SimpleTestDaoTransactionalAspectMethods().testMultipleReadsInOneTransactionCausesOnlyOneSelect();
    }

    @Test
    public void testMultipleReadsInNewTransactionsCausesNewSelect() {
        new SimpleTestDaoTransactionalAspectMethods().testMultipleReadsInNewTransactionsCausesNewSelect();
    }

    @Test
    public void testMultipleMergeInNewTransactionsDoesNotCreateInsert() {
        new SimpleTestDaoTransactionalAspectMethods().testMultipleMergeInNewTransactionsDoesNotCreateInsert();
    }

    @Test
    public void testMultipleReadOfSameObjectCausesChangesToBeReset() {
        new SimpleTestDaoTransactionalAspectMethods().testMultipleReadOfSameObjectCausesChangesToBeReset();
    }

    @Test
    public void testMagicalUpdateInvokedWithoutCallingWrite() {
        new SimpleTestDaoTransactionalAspectMethods().testMagicalUpdateInvokedWithoutCallingWrite();
    }

    @Test
    public void testUnicode() {
        new SimpleTestDaoTransactionalAspectMethods().testUnicode();
    }

    @Test
    public void testQueryDslJpa() {
        new SimpleTestDaoTransactionalAspectMethods().testQueryDslJpa();
    }

    @Test
    public void testTransactionRetry() {
        new SimpleTestDaoTransactionalAspectMethods().testTransactionRetry();
    }

}
