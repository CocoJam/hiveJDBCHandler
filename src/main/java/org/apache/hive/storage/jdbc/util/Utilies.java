package org.apache.hive.storage.jdbc.util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

import java.io.IOException;

public class Utilies {
    public static String getPasswdFromKeystore(String keystore, String key) throws IOException {
        String passwd = null;
        if (keystore != null && key != null) {
            Configuration conf = new Configuration();
            conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, keystore);
            char[] pwdCharArray = conf.getPassword(key);
            if (pwdCharArray != null) {
                passwd = new String(pwdCharArray);
            }
        }
        return passwd;
    }
}
