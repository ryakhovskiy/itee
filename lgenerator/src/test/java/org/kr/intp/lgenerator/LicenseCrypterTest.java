package org.kr.intp.lgenerator;

import junit.framework.TestCase;

import java.text.SimpleDateFormat;
import java.util.Date;

public class LicenseCrypterTest extends TestCase {

    public void testDecryptData() throws Exception {
        final String data = "my data to be encrypted";
        final String encrypted = LicenseCrypter.newInstance().encryptData(data);
        assertNotNull(encrypted);

        final String decrypted = LicenseCrypter.newInstance().decryptData(encrypted);
        assertEquals(data, decrypted);
    }
}