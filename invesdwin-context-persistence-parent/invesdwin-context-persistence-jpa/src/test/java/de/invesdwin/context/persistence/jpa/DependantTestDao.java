package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import de.invesdwin.context.persistence.jpa.api.dao.ADao;

@ThreadSafe
@Named
public class DependantTestDao extends ADao<DependantTestEntity> {

}
