package org.kr.intp.util.crypt;

import junit.framework.TestCase;

/**
 * Created by kr on 16.12.13.
 */
public class CrypterTest extends TestCase {

    public void testEncrypt() throws Exception {
        String data = "test string to be encrypted";

        Crypter crypter = Crypter.newInstance();

        byte[] encrypted = crypter.encrypt(data.getBytes());
        byte[] decryptedData = crypter.decrypt(encrypted);
        String decrypted = new String(decryptedData);
        assert data.equals(decrypted);

        encrypted = crypter.encrypt(data.getBytes("UTF8"));
        decryptedData = crypter.decrypt(encrypted);
        decrypted = new String(decryptedData);
        assert data.equals(decrypted);
    }
}
