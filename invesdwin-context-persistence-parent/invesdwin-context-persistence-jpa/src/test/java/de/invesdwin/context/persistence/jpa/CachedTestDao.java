package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.persistence.jpa.api.dao.ADao;
import jakarta.inject.Named;

@Named
@ThreadSafe
public class CachedTestDao extends ADao<CachedTestEntity> {

}
