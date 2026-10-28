package exchangeserver.gateway;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

/*

TLS for the gateway (D74): optional, enabled by giving the server a PKCS#12 keystore. Passwords come from the
environment (ES2_TLS_PASSWORD, ES2_TRUSTSTORE_PASSWORD), never from the command line, where other users of the
machine could read them in the process list.

 */
public final class Tls {

    public static final String[] PROTOCOLS = {"TLSv1.3", "TLSv1.2"};

    private Tls() {
    }

    public static SSLContext serverContext(Path keystore, char[] password) throws IOException {
        try {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(load(keystore, password), password);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(kmf.getKeyManagers(), null, null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new IOException("Cannot use keystore " + keystore + ": " + e.getMessage(), e);
        }
    }

    // truststore null: the JDK's default trusted certificates.
    public static SSLContext clientContext(Path truststore, char[] password) throws IOException {
        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(truststore == null ? null : load(truststore, password));
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, tmf.getTrustManagers(), null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new IOException("Cannot use truststore " + truststore + ": " + e.getMessage(), e);
        }
    }

    public static char[] passwordFromEnvironment(String variable) {
        String value = System.getenv(variable);
        if (value == null) {
            throw new IllegalStateException("Set the " + variable + " environment variable to the keystore password");
        }
        return value.toCharArray();
    }

    private static KeyStore load(Path file, char[] password) throws IOException, GeneralSecurityException {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(file)) {
            store.load(in, password);
        }
        return store;
    }
}
