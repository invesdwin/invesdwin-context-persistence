package de.invesdwin.context.persistence.jpa.api.index;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Index {
    String name() default "";

    boolean unique() default false;

    String[] columnNames();
}
