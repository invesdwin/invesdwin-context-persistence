package de.invesdwin.context.persistence.jpa.kundera;

import javax.annotation.concurrent.Immutable;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ SimpleTestDaoTest.class })
@Immutable
public class KunderaTestSuite {

}
