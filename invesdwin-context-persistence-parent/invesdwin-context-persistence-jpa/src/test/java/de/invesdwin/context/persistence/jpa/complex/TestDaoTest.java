package de.invesdwin.context.persistence.jpa.complex;

import javax.annotation.concurrent.ThreadSafe;
import javax.persistence.OptimisticLockException;
import javax.persistence.RollbackException;

import org.junit.jupiter.api.Test;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.TransactionSystemException;

import de.invesdwin.context.persistence.jpa.test.APersistenceTest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.assertions.Executable;
import de.invesdwin.util.error.Throwables;

@ThreadSafe
//@ContextConfiguration(locations = { APersistenzTest.CTX_TEST_SERVER }, inheritLocations = false)
public class TestDaoTest extends APersistenceTest {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clearAllTables();
    }

    @Test
    public void testDeleteWithoutTransaction() {
        new TestDaoTransactionalAspectMethods().testDeleteWithoutTransaction();
    }

    @Test
    public void testFlushWithoutTransaction() {
        Assertions.assertThrows(IllegalTransactionStateException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new TestDaoTransactionalAspectMethods().testFlushWithoutTransaction();
            }
        });
    }

    @Test
    public void testDeleteWithTransaction() {
        try {
            new TestDaoTransactionalAspectMethods().testDeleteWithTransaction();
        } catch (final TransactionSystemException e) {
            Assertions.assertThat(Throwables.isCausedByType(e, RollbackException.class)).isTrue();
        }
    }

    @Test
    public void testDeleteNonExisting() {
        new TestDaoTransactionalAspectMethods().testDeleteNonExisting();
    }

    @Test
    public void testDeleteByExample() {
        new TestDaoTransactionalAspectMethods().testDeleteByExample();
    }

    @Test
    public void testDeleteByIds() {
        new TestDaoTransactionalAspectMethods().testDeleteByIds();
    }

    @Test
    public void testDeleteByExampleSingle() {
        new TestDaoTransactionalAspectMethods().testDeleteByExampleSingle();
    }

    @Test
    public void testWriteAndRead() {
        new TestDaoTransactionalAspectMethods().testWriteAndRead();
    }

    @Test
    public void testQbeIdException() {
        Assertions.assertThrows(AssertionError.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new TestDaoTransactionalAspectMethods().testQbeIdException();
            }
        });
    }

    @Test
    public void testQbeIdDeleteException() {
        Assertions.assertThrows(AssertionError.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new TestDaoTransactionalAspectMethods().testQbeIdDeleteException();
            }
        });
    }

    @Test
    public void testQbePersistentException() {
        Assertions.assertThrows(AssertionError.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new TestDaoTransactionalAspectMethods().testQbePersistentException();
            }
        });
    }

    @Test
    public void testBeanValidation() {
        new TestDaoTransactionalAspectMethods().testBeanValidation();
    }

    @Test
    public void testIllegalId() {
        Assertions.assertThrows(AssertionError.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new TestDaoTransactionalAspectMethods().testIllegalId();
            }
        });
    }

    @Test
    public void testTransactionalMissing() {
        new TestDaoTransactionalAspectMethods().testTransactionalMissing();
    }

    @Test
    public void testTransactionalNotMissingOnRead() {
        new TestDaoTransactionalAspectMethods().testTransactionalNotMissingOnRead();
    }

    @Test
    public void testServiceTransactionalWithoutAnnotation() {
        new TestDaoTransactionalAspectMethods().testServiceTransactionalWithoutAnnotation();
    }

    @Test
    public void testOptimisticLocking() {
        Assertions.assertThrows(OptimisticLockException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new TestDaoTransactionalAspectMethods().testOptimisticLocking();
            }
        });
    }

    @Test
    public void testQueryWithNullParameter() {
        new TestDaoTransactionalAspectMethods().testQueryWithNullParameter();
    }

    @Test
    public void testRollback() {
        new TestDaoTransactionalAspectMethods().testRollback();
    }

    @Test
    public void testRequiresNewTransaction() {
        new TestDaoTransactionalAspectMethods().testRequiresNewTransaction();
    }

    @Test
    public void testMultipleReadsInOneTransactionCausesOnlyOneSelect() {
        new TestDaoTransactionalAspectMethods().testMultipleReadsInOneTransactionCausesOnlyOneSelect();
    }

    @Test
    public void testMultipleReadsInNewTransactionsCausesNewSelect() {
        new TestDaoTransactionalAspectMethods().testMultipleReadsInNewTransactionsCausesNewSelect();
    }

    @Test
    public void testMultipleMergeInNewTransactionsDoesNotCreateInsert() {
        new TestDaoTransactionalAspectMethods().testMultipleMergeInNewTransactionsDoesNotCreateInsert();
    }

    @Test
    public void testMultipleReadOfSameObjectCausesChangesToBeReset() {
        new TestDaoTransactionalAspectMethods().testMultipleReadOfSameObjectCausesChangesToBeReset();
    }

    @Test
    public void testMagicalUpdateInvokedWithoutCallingWrite() {
        new TestDaoTransactionalAspectMethods().testMagicalUpdateInvokedWithoutCallingWrite();
    }

    @Test
    public void testMergeFrom() {
        new TestDaoTransactionalAspectMethods().testMergeFrom();
    }

    @Test
    public void testUnicode() {
        new TestDaoTransactionalAspectMethods().testUnicode();
    }

    @Test
    public void testQueryDslJpa() {
        new TestDaoTransactionalAspectMethods().testQueryDslJpa();
    }

    @Test
    public void testTransactionRetry() {
        new TestDaoTransactionalAspectMethods().testTransactionRetry();
    }

}
