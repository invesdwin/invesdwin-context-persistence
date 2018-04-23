package de.invesdwin.context.persistence.jpa.eclipselink;

import javax.annotation.concurrent.NotThreadSafe;
import javax.persistence.Entity;

import de.invesdwin.context.persistence.jpa.api.PersistenceUnitName;
import de.invesdwin.context.persistence.jpa.api.dao.entity.AEntity;

/**
 * testing persistence unit redirection here for tests
 * 
 * @author subes
 * 
 */
@NotThreadSafe
@Entity
@PersistenceUnitName("another_pu")
public class AnotherPuTestEntity extends AEntity {

    private static final long serialVersionUID = 1L;

    private String name;

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

}
