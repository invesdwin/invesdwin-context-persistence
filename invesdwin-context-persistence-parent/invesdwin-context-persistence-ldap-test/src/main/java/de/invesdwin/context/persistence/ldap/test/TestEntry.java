package de.invesdwin.context.persistence.ldap.test;

import javax.annotation.concurrent.NotThreadSafe;
import javax.naming.Name;

import org.springframework.ldap.odm.annotations.Attribute;
import org.springframework.ldap.odm.annotations.DnAttribute;
import org.springframework.ldap.odm.annotations.Entry;
import org.springframework.ldap.odm.annotations.Id;

import de.invesdwin.util.bean.AValueObject;

@NotThreadSafe
@Entry(objectClasses = { "person", "top" })
public final class TestEntry extends AValueObject {

    @Id
    private Name dn;

    @Attribute(name = "cn")
    @DnAttribute(value = "cn", index = 0)
    private String fullName;

    private String sn;

    // No @Attribute annotation means this will be bound to the LDAP attribute
    // with the same value
    private String description;

    public Name getDn() {
        return dn;
    }

    public void setDn(final Name dn) {
        this.dn = dn;
    }

    public String getSn() {
        return sn;
    }

    public void setSn(final String sn) {
        this.sn = sn;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(final String fullName) {
        this.fullName = fullName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

}
