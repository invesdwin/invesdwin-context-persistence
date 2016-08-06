package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.NotThreadSafe;
import javax.persistence.Cacheable;
import javax.persistence.Entity;

import de.invesdwin.context.persistence.jpa.api.dao.entity.AEntity;

@SuppressWarnings("serial")
@Entity
@NotThreadSafe
@Cacheable
public class CachedTestEntity extends AEntity {

    private String name;

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

}
