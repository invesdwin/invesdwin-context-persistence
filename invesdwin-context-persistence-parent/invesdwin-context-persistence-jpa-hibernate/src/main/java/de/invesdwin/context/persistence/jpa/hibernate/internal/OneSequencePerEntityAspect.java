package de.invesdwin.context.persistence.jpa.hibernate.internal;

import java.util.Properties;

import javax.annotation.concurrent.NotThreadSafe;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.hibernate.id.enhanced.SequenceStyleGenerator;

@NotThreadSafe
@Aspect
public class OneSequencePerEntityAspect {

    @Around("execution(* org.hibernate.id.enhanced.SequenceStyleGenerator.configure(..))")
    public Object enableOneSequencePerEntity(final ProceedingJoinPoint pjp) throws Throwable {
        final Properties properties = (Properties) pjp.getArgs()[1];
        properties.put(SequenceStyleGenerator.CONFIG_PREFER_SEQUENCE_PER_ENTITY, true);
        return pjp.proceed();
    }

}
