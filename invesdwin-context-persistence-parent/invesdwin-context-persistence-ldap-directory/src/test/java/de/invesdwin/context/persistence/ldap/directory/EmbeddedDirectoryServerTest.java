package de.invesdwin.context.persistence.ldap.directory;

import java.io.File;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;

import org.junit.Test;
import org.springframework.core.io.FileSystemResource;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.security.kerberos.client.config.SunJaasKrb5LoginConfig;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.ldap.LdapProperties;
import de.invesdwin.context.persistence.ldap.directory.server.DirectoryServer;
import de.invesdwin.context.persistence.ldap.directory.test.DirectoryServerTest;
import de.invesdwin.context.security.kerberos.KerberosProperties;
import de.invesdwin.context.security.kerberos.Keytabs;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.lang.UUIDs;

@NotThreadSafe
@DirectoryServerTest
public class EmbeddedDirectoryServerTest extends ATest {

    @Inject
    private DirectoryServer directoryServer;
    @Inject
    private LdapTemplate ldapTemplate;

    @Test
    public void testLdapAuthenticate() throws InterruptedException {
        //since base context is set, login as admin is impossible through ldaptemplate
        Assertions.assertThat(ldapTemplate.authenticate("", "(uid=admin)", LdapProperties.LDAP_CONTEXT_PASSWORD))
                .isFalse();
        //but login as a normal user is possible
        final String uid = Strings.substringBefore(KerberosProperties.KERBEROS_SERVICE_PRINCIPAL, "/");
        Assertions.assertThat(
                ldapTemplate.authenticate("", "(uid=" + uid + ")", KerberosProperties.KERBEROS_SERVICE_PASSPHRASE))
                .isTrue();
        //though only with correct credentials
        Assertions.assertThat(ldapTemplate.authenticate("", "(uid=" + uid + ")",
                KerberosProperties.KERBEROS_SERVICE_PASSPHRASE + "2")).isFalse();
        Assertions.assertThat(
                ldapTemplate.authenticate("", "(uid=" + uid + "2)", KerberosProperties.KERBEROS_SERVICE_PASSPHRASE))
                .isFalse();
    }

    @Test
    public void testKerberosLogin() throws Exception {
        LoginContext loginContext = null;
        try {
            final String principal = "foo@INVESDWIN.DE";
            final File keytab = new File(ContextProperties.getCacheDirectory(), "foo.keytab");
            final String passphrase = UUIDs.newPseudorandomUUID();
            directoryServer.createKerberosPrincipal(principal, passphrase);
            Keytabs.createKeytab(principal, passphrase, keytab);

            final Set<Principal> principals = new HashSet<Principal>();
            principals.add(new KerberosPrincipal(principal));

            // client login
            Subject subject = new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());
            loginContext = new LoginContext("", subject, null, createClientConfig("foo", keytab));
            loginContext.login();
            subject = loginContext.getSubject();
            org.junit.Assert.assertEquals(1, subject.getPrincipals().size());
            org.junit.Assert.assertEquals(KerberosPrincipal.class,
                    subject.getPrincipals().iterator().next().getClass());
            org.junit.Assert.assertEquals(principal, subject.getPrincipals().iterator().next().getName());
            loginContext.logout();

            //client login with suffix
            subject = new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());
            loginContext = new LoginContext("", subject, null, createClientConfig("foo@INVESDWIN.DE", keytab));
            loginContext.login();
            subject = loginContext.getSubject();
            org.junit.Assert.assertEquals(1, subject.getPrincipals().size());
            org.junit.Assert.assertEquals(KerberosPrincipal.class,
                    subject.getPrincipals().iterator().next().getClass());
            org.junit.Assert.assertEquals(principal, subject.getPrincipals().iterator().next().getName());
            loginContext.logout();

            // server login
            subject = new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());
            loginContext = new LoginContext("", subject, null, createServerConfig("foo", keytab));
            loginContext.login();
            subject = loginContext.getSubject();
            org.junit.Assert.assertEquals(1, subject.getPrincipals().size());
            org.junit.Assert.assertEquals(KerberosPrincipal.class,
                    subject.getPrincipals().iterator().next().getClass());
            org.junit.Assert.assertEquals(principal, subject.getPrincipals().iterator().next().getName());
            loginContext.logout();

            // server login with suffix
            subject = new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());
            loginContext = new LoginContext("", subject, null, createServerConfig("foo@INVESDWIN.DE", keytab));
            loginContext.login();
            subject = loginContext.getSubject();
            org.junit.Assert.assertEquals(1, subject.getPrincipals().size());
            org.junit.Assert.assertEquals(KerberosPrincipal.class,
                    subject.getPrincipals().iterator().next().getClass());
            org.junit.Assert.assertEquals(principal, subject.getPrincipals().iterator().next().getName());
            loginContext.logout();
        } finally {
            if (loginContext != null) {
                loginContext.logout();
            }
        }
    }

    private SunJaasKrb5LoginConfig createServerConfig(final String servicePrincipal, final File keytab)
            throws Exception {
        final SunJaasKrb5LoginConfig config = createConfig(servicePrincipal, keytab);
        config.setIsInitiator(false);
        return config;
    }

    private SunJaasKrb5LoginConfig createClientConfig(final String servicePrincipal, final File keytab)
            throws Exception {
        final SunJaasKrb5LoginConfig config = createConfig(servicePrincipal, keytab);
        config.setIsInitiator(true);
        return config;
    }

    private SunJaasKrb5LoginConfig createConfig(final String servicePrincipal, final File keytab) throws Exception {
        final SunJaasKrb5LoginConfig config = new SunJaasKrb5LoginConfig();
        config.setKeyTabLocation(new FileSystemResource(keytab));
        config.setServicePrincipal(servicePrincipal);
        config.setDebug(KerberosProperties.KERBEROS_DEBUG);
        config.afterPropertiesSet();
        return config;
    }

}
