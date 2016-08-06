package de.invesdwin.context.persistence.jpa.hibernate;

import javax.annotation.concurrent.Immutable;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import de.invesdwin.context.persistence.jpa.api.bulkinsert.BulkInsertEntitiesTest;

@RunWith(Suite.class)
@SuiteClasses({ BulkInsertEntitiesTest.class })
@Immutable
public class MySqlLoadDataInfileTestSuite {

}