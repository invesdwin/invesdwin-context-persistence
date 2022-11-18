package de.invesdwin.context.persistence.jpa.simple;

import javax.annotation.concurrent.ThreadSafe;

import org.junit.jupiter.api.Test;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.TransactionSystemException;

import de.invesdwin.context.persistence.jpa.test.APersistenceTest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.assertions.Executable;
import de.invesdwin.util.bean.AValueObject;
import de.invesdwin.util.error.Throwables;
import jakarta.persistence.RollbackException;

@ThreadSafe
//@ContextConfiguration(locations = { APersistenzTest.CTX_TEST_SERVER }, inheritLocations = false)
public class SimpleTestDaoTest extends APersistenceTest {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clearAllTables();
    }

    @Test
    public void testDeleteWithoutTransaction() {
        new SimpleTestDaoTransactionalAspectMethods().testDeleteWithoutTransaction();
    }

    @Test
    public void testFlushWithoutTransaction() {
        Assertions.assertThrows(IllegalTransactionStateException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new SimpleTestDaoTransactionalAspectMethods().testFlushWithoutTransaction();
            }
        });
    }

    @Test
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
        Assertions.assertThrows(AssertionError.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new SimpleTestDaoTransactionalAspectMethods().testQbeIdException();
            }
        });
    }

    @Test
    public void testQbeIdDeleteException() {
        Assertions.assertThrows(AssertionError.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new SimpleTestDaoTransactionalAspectMethods().testQbeIdDeleteException();
            }
        });
    }

    @Test
    public void testQbePersistentException() {
        Assertions.assertThrows(AssertionError.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new SimpleTestDaoTransactionalAspectMethods().testQbePersistentException();
            }
        });
    }

    @Test
    public void testBeanValidation() {
        new SimpleTestDaoTransactionalAspectMethods().testBeanValidation();
    }

    @Test
    public void testIllegalId() {
        Assertions.assertThrows(AssertionError.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                new SimpleTestDaoTransactionalAspectMethods().testIllegalId();
            }
        });
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
    public void testMergeFrom() {
        new SimpleTestDaoTransactionalAspectMethods().testMergeFrom();
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

    public static class TestVO extends AValueObject {
        private static final long serialVersionUID = 1L;
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

}
