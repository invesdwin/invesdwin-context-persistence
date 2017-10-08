package de.invesdwin.context.persistence.jpa.kundera;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
@SelectClasses({ SimpleTestDaoTest.class })
@Immutable
public class KunderaTestSuite {

}
