package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.NotThreadSafe;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.OneToOne;

import de.invesdwin.context.persistence.jpa.api.dao.entity.AEntity;
import de.invesdwin.context.persistence.jpa.complex.TestEntity;

@SuppressWarnings("serial")
@Entity
@NotThreadSafe
public class DependantTestEntity extends AEntity {

    @OneToOne(cascade = { CascadeType.REMOVE })
    private TestEntity vater;
    private String name;

    public TestEntity getVater() {
        return vater;
    }

    public void setVater(final TestEntity vater) {
        this.vater = vater;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

}
