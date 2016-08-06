package de.invesdwin.context.persistence.jpa.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation can be used in tests to change the persistence context configuration to switch between memory and
 * server mode.
 * 
 * The uppermost annotation wins, thus it is possible to override the configuration of base classes.
 * 
 */
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface PersistenceTest {

    PersistenceTestContext value() default PersistenceTestContext.MEMORY;

}
