package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.jpa.api.dao.ADao;
import jakarta.inject.Named;

@ThreadSafe
@Named
public class DependantTestDao extends ADao<DependantTestEntity> {

}
