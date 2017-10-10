package de.invesdwin.context.persistence.jpa.hibernate;

import javax.annotation.concurrent.Immutable;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import de.invesdwin.context.persistence.jpa.PersistenceTestSuite;
import de.invesdwin.context.persistence.jpa.hibernate.internal.OneSequencePerEntityAspectTest;

@RunWith(Suite.class)
@SuiteClasses({ MultiplePersistenceUnitsTest.class, PersistenceTestSuite.class, OneSequencePerEntityAspectTest.class })
@Immutable
public class HibernateTestSuite {

}
