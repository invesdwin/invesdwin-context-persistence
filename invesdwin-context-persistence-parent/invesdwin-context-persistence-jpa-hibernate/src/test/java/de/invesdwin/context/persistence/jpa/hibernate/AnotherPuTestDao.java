package de.invesdwin.context.persistence.jpa.hibernate;

import javax.annotation.concurrent.ThreadSafe;
import jakarta.inject.Named;

import de.invesdwin.context.persistence.jpa.api.dao.ADao;

@ThreadSafe
@Named
public class AnotherPuTestDao extends ADao<AnotherPuTestEntity> {

}
