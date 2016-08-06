package de.invesdwin.context.persistence.ldap.test;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import org.junit.Test;
import org.springframework.dao.EmptyResultDataAccessException;

import de.invesdwin.context.persistence.ldap.directory.test.DirectoryServerTest;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
@DirectoryServerTest
public class TestDaoTest extends ATest {

    @Inject
    private TestDao testDao;

    @Test
    public void test() throws InterruptedException {
        Assertions.assertThat(testDao.count()).isEqualTo(3);

        Assertions.assertThat(testDao.getBaseLdapPath().toString()).isEqualTo("dc=invesdwin,dc=de");

        final TestEntry entry = new TestEntry();
        entry.setDescription("some description bla bla bla");
        entry.setFullName("some full name");
        entry.setSn("some surname");
        Assertions.assertThat(entry.getDn()).isNull();
        testDao.save(entry);
        Assertions.assertThat(entry.getDn().toString()).isEqualTo("cn=some full name");

        Assertions.assertThat(testDao.count()).isEqualTo(4);

        log.info("%s", testDao.findAll());

        final TestEntry entryFound = testDao.findUserBySurname(entry.getSn());
        Assertions.assertThat(entryFound.getFullName()).isEqualTo(entry.getFullName());
        Assertions.assertThat(entryFound.getDn()).isEqualTo(entry.getDn());

        testDao.delete(entry);
        Assertions.assertThat(testDao.count()).isEqualTo(3);

        final TestEntry entryToo = new TestEntry();
        entryToo.setDescription("some description bla bla bla too");
        entryToo.setFullName("some full name too");
        entryToo.setSn("some surname too");
        try {
            testDao.saveWithExceptionInTransaction(entryToo);
            Assertions.fail("Exception expected");
        } catch (final RuntimeException e) {
            Assertions.assertThat(e.getMessage()).isEqualTo("transaction rollback reason");
        }
        Assertions.assertThat(entryToo.getDn()).isNotNull();
        Assertions.assertThat(testDao.count()).isEqualTo(3);

        final TestEntry entryThree = new TestEntry();
        entryThree.setDescription("some description bla bla bla three");
        entryThree.setFullName("some full name three");
        entryThree.setSn("some surname three");
        testDao.saveInTransaction(entryThree);
        Assertions.assertThat(testDao.count()).isEqualTo(4);

        entryToo.setDn(null);
        testDao.saveInTransaction(entryToo);
        Assertions.assertThat(testDao.count()).isEqualTo(5);

        final TestEntry entryThreeFound = testDao.findUserBySurname(entryThree.getSn());
        Assertions.assertThat(entryThreeFound.getFullName()).isEqualTo(entryThree.getFullName());
        Assertions.assertThat(entryThreeFound.getDn()).isEqualTo(entryThree.getDn());

        try {
            testDao.findUserBySurname("invalid");
            Assertions.fail("exception expected");
        } catch (final EmptyResultDataAccessException t) {
            Assertions.assertThat(t.getMessage()).isEqualTo("Incorrect result size: expected 1, actual 0");
        }

    }
}
