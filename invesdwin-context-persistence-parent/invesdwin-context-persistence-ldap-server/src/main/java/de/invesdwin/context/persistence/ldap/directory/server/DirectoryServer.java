package de.invesdwin.context.persistence.ldap.directory.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.entry.Modification;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.server.annotations.CreateKdcServer;
import org.apache.directory.server.core.api.CoreSession;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.factory.DSAnnotationProcessor;
import org.apache.directory.server.factory.ServerAnnotationProcessor;
import org.apache.directory.server.kerberos.kdc.KdcServer;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.protocol.shared.transport.Transport;
import org.apache.directory.shared.kerberos.codec.types.EncryptionType;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.ldap.LdapProperties;
import de.invesdwin.context.persistence.ldap.directory.server.internal.DirectoryServerConfigAnnotations;
import de.invesdwin.context.security.kerberos.KerberosProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Reflections;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.lang.UUIDs;
import de.invesdwin.util.shutdown.IShutdownHook;
import de.invesdwin.util.shutdown.ShutdownHookManager;

@ThreadSafe
public class DirectoryServer {

    private final Log log = new Log(this);

    private DirectoryService directoryService;
    private LdapServer ldapServer;

    private final IShutdownHook shutdownHook = new IShutdownHook() {
        @Override
        public void shutdown() throws Exception {
            stop();
        }
    };
    private KdcServer kdcServer;

    //http://mail-archives.apache.org/mod_mbox/directory-users/201302.mbox/%3CCABzFU-d+diNYHUhoKfDg+DD6ymsUzYyOpbaU7B3As2qPwHBuAA@mail.gmail.com%3E
    //http://svn.apache.org/repos/asf/directory/apacheds/trunk/core-annotations/src/main/java/org/apache/directory/server/core/factory/DefaultDirectoryServiceFactory.java
    //https://cwiki.apache.org/confluence/display/DIRxSRVx11/4.3.+Embedding+ApacheDS+as+a+Web+Application
    public synchronized void start() throws Exception {
        Assertions.assertThat(directoryService).as("%s is already running!", getClass().getSimpleName()).isNull();

        logServerStarting();

        final DirectoryServerConfigAnnotations config = new DirectoryServerConfigAnnotations();

        directoryService = DSAnnotationProcessor.createDS(config.newCreateDSAnnotation());
        Assertions.assertThat(directoryService.isStarted()).isTrue();

        //change admin password
        changeAdminPassword();

        createKerberosUsers();
        kdcServer = Reflections.method("createKdcServer")
                .withReturnType(KdcServer.class)
                .withParameterTypes(CreateKdcServer.class, DirectoryService.class)
                .in(ServerAnnotationProcessor.class)
                .invoke(config.newCreateKdcServerAnnotation(), directoryService);

        //timestamp verificaton makes debugging harder, since every second request is the actual one
        kdcServer.getConfig().setPaEncTimestampRequired(false);
        if (kdcServer.getChangePwdServer() != null) {
            kdcServer.getChangePwdServer().getConfig().setPaEncTimestampRequired(false);
        }

        final Set<EncryptionType> encryptionTypes = KerberosProperties.getEncryptionTypes();
        kdcServer.getConfig().setEncryptionTypes(encryptionTypes);
        for (final Transport transport : kdcServer.getTransports()) {
            Assertions.assertThat(transport.getAcceptor().isActive()).isTrue();
        }

        ldapServer = ServerAnnotationProcessor.instantiateLdapServer(config.newCreateLdapServerAnnotation(),
                directoryService);
        Assertions.assertThat(ldapServer.isStarted()).isFalse();
        ldapServer.start();
        Assertions.assertThat(ldapServer.isStarted()).isTrue();

        ShutdownHookManager.register(shutdownHook);
    }

    public void loadLdifResource(final Resource resource) {
        try {
            final CoreSession coreSession = getDirectoryService().getAdminSession();
            final InputStream in = resource.getInputStream();
            for (final LdifEntry ldifEntry : new LdifReader(in)) {
                final Dn dn = ldifEntry.getDn();

                if (ldifEntry.isEntry()) {
                    final Entry entry = ldifEntry.getEntry();
                    try {
                        coreSession.lookup(dn);
                    } catch (final Exception e) {
                        coreSession.add(new DefaultEntry(coreSession.getDirectoryService().getSchemaManager(), entry));
                    }
                } else {
                    //modify
                    final List<Modification> items = ldifEntry.getModifications();
                    coreSession.modify(dn, items);
                }
            }
            in.close();
        } catch (final LdapException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void loadLdifContent(final String content) {
        loadLdifResource(new ByteArrayResource(content.getBytes()));
    }

    private void changeAdminPassword() {
        //dn: uid=admin,ou=system
        //changetype: modify
        //replace: userPassword
        //userPassword: invesdwin
        //-

        final String content = "dn: uid=admin,ou=system\n" + "changetype: modify\n" + "replace: userPassword\n"
                + "userPassword: " + LdapProperties.LDAP_CONTEXT_PASSWORD + "\n" + "-";
        loadLdifContent(content);
    }

    private void createKerberosUsers() {
        //        + "dn: uid="
        //                + uid
        //                + ",ou=users,"
        //                + LdapProperties.LDAP_CONTEXT_BASE
        //                + "\n"//
        //                + "objectClass: top\n"//
        //                + "objectClass: person\n"//
        //                + "objectClass: inetOrgPerson\n"//
        //                + "objectClass: krb5principal\n"//
        //                + "objectClass: krb5kdcentry\n"//
        //                + "cn: "
        //                + uid
        //                + "\n"//
        //                + "sn: Service\n"//
        //                + "uid: "
        //                + uid
        //                + "\n"//
        //                + "userPassword: "
        //                + KerberosProperties.KERBEROS_SERVICE_PASSPHRASE
        //                + "\n"//
        //                + "krb5PrincipalName: "
        //                + kerberosServicePrincipal
        //                + "\n"//
        //                + "krb5KeyVersionNumber: 0\n"//
        //                + "\n"//
        createKerberosPrincipal(KerberosProperties.KERBEROS_SERVICE_PRINCIPAL,
                KerberosProperties.KERBEROS_SERVICE_PASSPHRASE);
        //                //dn: uid=krbtgt,ou=users,dc=example,dc=com
        //                //objectClass: top
        //                //objectClass: person
        //                //objectClass: inetOrgPerson
        //                //objectClass: krb5principal
        //                //objectClass: krb5kdcentry
        //                //cn: KDC Service
        //                //sn: Service
        //                //uid: krbtgt
        //                //userPassword: secret
        //                //krb5PrincipalName: krbtgt/EXAMPLE.COM@EXAMPLE.COM
        //                //krb5KeyVersionNumber: 0
        //                + "dn: uid=krbtgt,ou=users,"
        //                + LdapProperties.LDAP_CONTEXT_BASE
        //                + "\n"//
        //                + "objectClass: top\n"//
        //                + "objectClass: person\n"//
        //                + "objectClass: inetOrgPerson\n"//
        //                + "objectClass: krb5principal\n"//
        //                + "objectClass: krb5kdcentry\n"//
        //                + "cn: KDC Service\n"//
        //                + "sn: Service\n"//
        //                + "uid: krbtgt\n"//
        //                + "userPassword: "
        //                + KerberosProperties.KERBEROS_SERVICE_PASSPHRASE
        //                + "\n"//
        //                + "krb5PrincipalName: krbtgt/"
        //        + KerberosProperties.KERBEROS_PRIMARY_REALM
        //                + "@"
        //        + KerberosProperties.KERBEROS_PRIMARY_REALM
        //                + "\n"//
        //                + "krb5KeyVersionNumber: 0\n"//
        //                + "\n"//
        createKerberosPrincipal(
                "krbtgt/" + KerberosProperties.KERBEROS_PRIMARY_REALM + "@" + KerberosProperties.KERBEROS_PRIMARY_REALM,
                UUIDs.newPseudorandomUUID());
        //                //dn: uid=ldap,ou=users,dc=example,dc=com
        //                //objectClass: top
        //                //objectClass: person
        //                //objectClass: inetOrgPerson
        //                //objectClass: krb5principal
        //                //objectClass: krb5kdcentry
        //                //cn: LDAP
        //                //sn: Service
        //                //uid: ldap
        //                //userPassword: secret
        //                //krb5PrincipalName: ldap/localhost@EXAMPLE.COM
        //                //krb5KeyVersionNumber: 0
        //                + "dn: uid=ldap,ou=users," + LdapProperties.LDAP_CONTEXT_BASE
        //                + "\n"//
        //                + "objectClass: top\n"//
        //                + "objectClass: person\n"//
        //                + "objectClass: inetOrgPerson\n"//
        //                + "objectClass: krb5principal\n"//
        //                + "objectClass: krb5kdcentry\n"//
        //                + "cn: LDAP\n"//
        //                + "sn: Service\n"//
        //                + "uid: ldap\n"//
        //                + "userPassword: " + KerberosProperties.KERBEROS_SERVICE_PASSPHRASE
        //                + "\n"//
        //                + "krb5PrincipalName: ldap/" + KerberosProperties.KERBEROS_SERVER_URI.getHost() + "@"
        //                + KerberosProperties.KERBEROS_PRIMARY_REALM + "\n"//
        //                + "krb5KeyVersionNumber: 0\n"
        createKerberosPrincipal("ldap/" + KerberosProperties.KERBEROS_SERVER_URI.getHost() + "@"
                + KerberosProperties.KERBEROS_PRIMARY_REALM, UUIDs.newPseudorandomUUID());
    }

    private void logServerStarting() {
        log.info("Starting directory server at: %s", LdapProperties.LDAP_CONTEXT_URI);
        log.info("Starting kerberos server at: %s", KerberosProperties.KERBEROS_SERVER_URI);
    }

    public synchronized DirectoryService getDirectoryService() {
        assertServiceRunning();
        return directoryService;
    }

    /**
     * Creates a principal in the KDC with the specified user and password.
     */
    public synchronized void createKerberosPrincipal(final String principalName, final String passPhrase) {
        Assertions.assertThat(principalName).endsWith("@" + KerberosProperties.KERBEROS_PRIMARY_REALM);
        final String uid = Strings.removeEnd(Strings.substringBefore(principalName, "/"),
                "@" + KerberosProperties.KERBEROS_PRIMARY_REALM);
        final String baseDn = "ou=users," + LdapProperties.LDAP_CONTEXT_BASE;
        final String content = "dn: uid=" + uid + "," + baseDn + "\n" + "objectClass: top\n" + "objectClass: person\n"
                + "objectClass: inetOrgPerson\n" + "objectClass: krb5principal\n" + "objectClass: krb5kdcentry\n"
                + "cn: " + uid + "\n" + "sn: " + uid + "\n" + "uid: " + uid + "\n" + "userPassword: " + passPhrase
                + "\n" + "krb5PrincipalName: " + principalName + "\n" + "krb5KeyVersionNumber: 0";
        loadLdifContent(content);
    }

    public synchronized LdapServer getLdapServer() {
        assertServiceRunning();
        return ldapServer;
    }

    public synchronized KdcServer getKdcServer() {
        assertServiceRunning();
        return kdcServer;
    }

    private void assertServiceRunning() {
        Assertions.assertThat(directoryService).as("%s is not running!", getClass().getSimpleName()).isNotNull();
    }

    public synchronized void stop() {
        if (directoryService != null) {
            ShutdownHookManager.unregister(shutdownHook);
            kdcServer.stop();
            kdcServer = null;
            ldapServer.stop();
            ldapServer = null;
            try {
                directoryService.shutdown();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
            directoryService = null;
        }
    }

}
