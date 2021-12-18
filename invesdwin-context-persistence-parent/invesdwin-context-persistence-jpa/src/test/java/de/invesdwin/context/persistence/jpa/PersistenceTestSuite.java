package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.persistence.jpa.complex.TestDaoTest;
import de.invesdwin.context.persistence.jpa.simple.SimpleTestDaoTest;

/**
 * This TestSuite will not run without an ORM provider module, thus only to be used from inside one of those.
 */
@Suite
@SelectClasses({ TestDaoPerformanceTest.class, TestDaoTest.class, CachedTestDaoTest.class, DependantTestDaoTest.class,
        TestRepositoryTest.class, SimpleTestDaoTest.class })
@Immutable
public class PersistenceTestSuite {

}
