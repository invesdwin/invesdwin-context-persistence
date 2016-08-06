package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import de.invesdwin.context.persistence.jpa.api.dao.ADao;

@Named
@ThreadSafe
public class CachedTestDao extends ADao<CachedTestEntity> {

}
