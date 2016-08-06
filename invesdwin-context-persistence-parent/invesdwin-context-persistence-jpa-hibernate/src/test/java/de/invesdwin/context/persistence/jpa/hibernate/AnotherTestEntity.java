package de.invesdwin.context.persistence.jpa.hibernate;

import javax.annotation.concurrent.NotThreadSafe;
import javax.persistence.Entity;

import de.invesdwin.context.persistence.jpa.api.dao.entity.AEntity;

@NotThreadSafe
@Entity
public class AnotherTestEntity extends AEntity {

    private static final long serialVersionUID = 1L;

    private String name;

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

}
