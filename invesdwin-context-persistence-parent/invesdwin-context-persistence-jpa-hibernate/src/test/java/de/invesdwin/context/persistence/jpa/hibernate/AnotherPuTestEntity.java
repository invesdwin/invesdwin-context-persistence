package de.invesdwin.context.persistence.jpa.hibernate;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.jpa.api.PersistenceUnitName;
import de.invesdwin.context.persistence.jpa.api.dao.entity.AEntity;
import jakarta.persistence.Entity;

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
