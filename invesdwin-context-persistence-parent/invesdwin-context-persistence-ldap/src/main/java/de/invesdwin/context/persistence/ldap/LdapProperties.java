package de.invesdwin.context.persistence.ldap;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.context.system.properties.SystemProperties;

@Immutable
public final class LdapProperties {

    public static final URI LDAP_CONTEXT_URI;
    public static final String LDAP_CONTEXT_BASE;
    public static final String LDAP_CONTEXT_USERNAME;
    public static final String LDAP_CONTEXT_PASSWORD;

    static {
        final SystemProperties systemProperties = new SystemProperties(LdapProperties.class);
        LDAP_CONTEXT_URI = systemProperties.getURI("LDAP_CONTEXT_URI", true);
        LDAP_CONTEXT_BASE = systemProperties.getString("LDAP_CONTEXT_BASE");
        LDAP_CONTEXT_USERNAME = systemProperties.getString("LDAP_CONTEXT_USERNAME");
        LDAP_CONTEXT_PASSWORD = systemProperties.getStringWithSecurityWarning("LDAP_CONTEXT_PASSWORD",
                IProperties.INVESDWIN_DEFAULT_PASSWORD);
    }

    private LdapProperties() {}

}
