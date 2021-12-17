package de.invesdwin.context.persistence.jpa.datanucleus;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.validation.constraints.NotNull;

import org.apache.commons.math3.random.RandomDataGenerator;

import de.invesdwin.context.persistence.jpa.api.dao.entity.IEntity;
import de.invesdwin.context.persistence.jpa.api.index.Index;
import de.invesdwin.context.persistence.jpa.api.index.Indexes;
import de.invesdwin.norva.beanpath.annotation.Hidden;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Objects;

/**
 * Datanucleus Enhancer has problems with APropertySupport having some Norva-Hidden annotations; so we cannot use
 * AEntity or AValueObject here until this is fixed, anyway just like with kundera we would only use Datanucleus for
 * NoSQL anyway where the AEntity base class would provide too many features.
 * 
 * @author subes
 *
 */
@NotThreadSafe
@Entity
@Indexes({ @Index(columnNames = { "name" }) })
public class SimpleTestEntity implements IEntity, Cloneable, Serializable, Comparable<Object> {

    private static final long serialVersionUID = 1L;

    @Id
    @NotNull
    private Long id;

    @NotNull
    private String name;

    @Override
    @Hidden(skip = true)
    public Long getId() {
        return id;
    }

    @Override
    public void setId(final Long id) {
        Assertions.assertThat(id).as("Id cannot be reset.").isNotNull();
        this.id = id;
    }

    //CHECKSTYLE:OFF super.clone
    @Override
    public SimpleTestEntity clone() {
        //CHECKSTYLE:ON
        return Objects.deepClone(this);
    }

    /**
     * If id is set, the identity is specified by it and by the class.
     */
    @Override
    public int hashCode() {
        if (id != null) {
            return this.getClass().hashCode() + id.hashCode();
        } else {
            return super.hashCode();
        }
    }

    /**
     * Matches id match if available, otherwise all attributes.
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof SimpleTestEntity && id != null) {
            final SimpleTestEntity cObj = (SimpleTestEntity) obj;
            return Objects.equals(id, cObj.id);
        } else {
            return super.equals(obj);
        }
    }

    @PrePersist
    protected void prePersist() {
        if (id == null) {
            id = new RandomDataGenerator().nextLong(0, Long.MAX_VALUE);
        }
    }

    @PreUpdate
    protected void preUpdate() {
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Override
    public int compareTo(final Object o) {
        return Objects.reflectionCompareTo(this, o);
    }

}
