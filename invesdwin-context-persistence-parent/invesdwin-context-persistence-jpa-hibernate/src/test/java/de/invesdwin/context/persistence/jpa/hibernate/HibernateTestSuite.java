package de.invesdwin.context.persistence.jpa.hibernate;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.runner.RunWith;

import de.invesdwin.context.persistence.jpa.PersistenceTestSuite;
import de.invesdwin.context.persistence.jpa.hibernate.internal.OneSequencePerEntityAspectTest;

@RunWith(JUnitPlatform.class)
@SelectClasses({ MultiplePersistenceUnitsTest.class, PersistenceTestSuite.class, OneSequencePerEntityAspectTest.class })
@Immutable
public class HibernateTestSuite {

}
