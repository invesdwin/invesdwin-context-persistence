package de.invesdwin.context.persistence.jpa.hibernate;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Named;

@Named
@NotThreadSafe
public class HibernateExceptionTranslatorBean extends org.springframework.orm.hibernate5.HibernateExceptionTranslator {

}
