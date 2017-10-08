package de.invesdwin.context.persistence.jpa.hibernate;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.runner.RunWith;

import de.invesdwin.context.persistence.jpa.api.bulkinsert.BulkInsertEntitiesTest;

@RunWith(JUnitPlatform.class)
@SelectClasses({ BulkInsertEntitiesTest.class })
@Immutable
public class MySqlLoadDataInfileTestSuite {

}