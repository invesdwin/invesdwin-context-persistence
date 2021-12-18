package de.invesdwin.context.persistence.jpa.hibernate;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.persistence.jpa.api.bulkinsert.BulkInsertEntitiesTest;

@Suite
@SelectClasses({ BulkInsertEntitiesTest.class })
@Immutable
public class MySqlLoadDataInfileTestSuite {

}