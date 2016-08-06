package de.invesdwin.context.persistence.ldap.directory.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * With this annotation you can mark tests so that they enable the directory server during test execution.
 * 
 * You can also disable the directory server by putting "false" as the value to override an enabled directory server
 * from a base class.
 */
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface DirectoryServerTest {

    boolean value() default true;

}
