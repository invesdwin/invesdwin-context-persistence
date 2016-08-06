package de.invesdwin.context.persistence.jpa.complex;

import javax.annotation.concurrent.NotThreadSafe;
import javax.persistence.Entity;
import javax.validation.constraints.NotNull;

import de.invesdwin.context.persistence.jpa.api.dao.entity.AEntity;
import de.invesdwin.context.persistence.jpa.api.index.Index;
import de.invesdwin.context.persistence.jpa.api.index.Indexes;

@Entity
@NotThreadSafe
@Indexes({ @Index(columnNames = { "name" }) })
public class TestEntity extends AEntity {

    private static final long serialVersionUID = 1L;

    @NotNull
    private String name;

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

}
