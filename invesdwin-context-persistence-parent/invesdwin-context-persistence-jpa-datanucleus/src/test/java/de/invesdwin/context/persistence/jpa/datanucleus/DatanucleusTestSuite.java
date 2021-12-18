package de.invesdwin.context.persistence.jpa.datanucleus;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ SimpleTestDaoTest.class })
@Immutable
public class DatanucleusTestSuite {

}
