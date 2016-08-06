package de.invesdwin.context.persistence.ldap.test;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;
import javax.naming.ldap.LdapName;

import org.springframework.ldap.repository.support.QueryDslLdapQuery;
import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.persistence.ldap.dao.ALdapDao;

@Named
@ThreadSafe
public class TestDao extends ALdapDao<TestEntry> {

    public TestEntry findUserBySurname(final String sn) {
        final QTestEntry qTest = QTestEntry.testEntry;
        return new QueryDslLdapQuery<TestEntry>(getLdapTemplate(), qTest).where(qTest.sn.eq(sn)).uniqueResult();
    }

    @Transactional
    public void saveInTransaction(final TestEntry e) {
        save(e);
    }

    @Override
    protected LdapName getBaseLdapPath() {
        return super.getBaseLdapPath();
    }

    @Transactional
    public void saveWithExceptionInTransaction(final TestEntry e) {
        save(e);
        throw new RuntimeException("transaction rollback reason");
    }

}
