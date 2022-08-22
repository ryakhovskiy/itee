package org.kr.intp.util.crypt;

import org.junit.Assert;
import org.junit.Test;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

/**
 * Created by kr on 4/15/2014.
 */
public class LicenseCrypterTest extends Assert {

    private final Logger log = LoggerFactory.getLogger(LicenseCrypterTest.class);
    private final LicenseCrypter licenseCrypter = LicenseCrypter.newInstance();

    @Test
    public void testEncryptDecryptData() {
        String data = "data";
        log.debug("encrypting data: " + data);
        String encrypted = licenseCrypter.encryptData(data);
        log.debug("encrypted data: " + encrypted);
        String decrypted = licenseCrypter.decryptData(encrypted);
        log.debug("decrypted data: " + decrypted);
        assertEquals("Encrypted and Decrypted values are not the same!", data, decrypted);
    }

    @Test(expected = RuntimeException.class)
    public void testDecryptWrongData() {
        String data = "---";
        licenseCrypter.decryptData(data);
    }

    @Test(expected = RuntimeException.class)
    public void testDecryptEmptyString() throws Exception {
        String data = "";
        licenseCrypter.decryptData(data);
    }

    @Test(expected = NullPointerException.class)
    public void testDecryptNull() throws Exception {
        String data = null;
        licenseCrypter.decryptData(data);
    }
}
