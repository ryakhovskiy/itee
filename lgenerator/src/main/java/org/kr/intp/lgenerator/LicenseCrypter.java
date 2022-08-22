package org.kr.intp.lgenerator;

import java.nio.charset.Charset;
import java.util.Base64;

/**
 * Created by kr on 01.04.2014.
 */
public final class LicenseCrypter {

    public static LicenseCrypter newInstance() {
        return new LicenseCrypter();
    }

    private final Charset defaultEncoding = Charset.forName("UTF8");
    private final Base64.Encoder encoder = Base64.getEncoder();
    private final Base64.Decoder decoder = Base64.getDecoder();
    private final Crypter crypter;

    private LicenseCrypter() {
        this.crypter = Crypter.newInstance();
    }

    public String decryptData(String data) {
        try {
            final byte[] encrypted = decoder.decode(data);
            final byte[] decrypted = crypter.decrypt(encrypted);
            return new String(decrypted, defaultEncoding);
        } catch (Exception e) {
            System.err.printf(e.getMessage());
            throw new java.lang.RuntimeException(e);
        }
    }

    public String encryptData(String data) {
        try {
            final byte[] encrypted = crypter.encrypt(data.getBytes(defaultEncoding));
            return encoder.encodeToString(encrypted);
        } catch (Exception e) {
            System.err.printf(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
