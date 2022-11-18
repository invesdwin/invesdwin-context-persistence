package de.invesdwin.context.persistence.jpa.hibernate;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.persistence.jpa.PersistenceTestSuite;

@Suite
@SelectClasses({ MultiplePersistenceUnitsTest.class, PersistenceTestSuite.class })
@Immutable
public class HibernateTestSuite {

}
