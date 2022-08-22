package org.kr.itee.perfmon.ws.utils;

import com.sun.crypto.provider.SunJCE;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.Key;
import java.security.spec.KeySpec;
import java.util.Base64;

/**
 * Created by kr on 6/27/2014.
 */
public class Crypter {

    private static final Crypter crypter = new Crypter();
    public static Crypter getCrypter() {
        return crypter;
    }

    private Crypter() { }

    private Cipher getCipher(int mode) throws Exception {
        final Cipher c = Cipher.getInstance("AES/CBC/PKCS5Padding", new SunJCE());
        final byte[] iv = "e675f725e675f725".getBytes("UTF-8");
        c.init(mode, generateKey(), new IvParameterSpec(iv));
        return c;
    }

    private Key generateKey() throws Exception {
        final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
        final char[] password = "De10itte@G".toCharArray();
        final byte[] salt = "1NtimeM@n@9ement".getBytes("UTF-8");

        final KeySpec spec = new PBEKeySpec(password, salt, 65536, 128);
        final SecretKey tmp = factory.generateSecret(spec);
        final byte[] encoded = tmp.getEncoded();
        return new SecretKeySpec(encoded, "AES");
    }

    public String encrypt(byte[] raw) throws Exception {
        final Cipher c = getCipher(Cipher.ENCRYPT_MODE);
        byte[] encrypted = c.doFinal(raw);
        byte[] encoded = Base64.getEncoder().encode(encrypted);
        return new String(encoded);
    }

    public String encrypt(String data) throws Exception {
        final byte[] raw = data.getBytes("UTF8");
        final Cipher c = getCipher(Cipher.ENCRYPT_MODE);
        byte[] encrypted = c.doFinal(raw);
        byte[] encoded = Base64.getEncoder().encode(encrypted);
        return new String(encoded);
    }

    public static void main(String... args) throws Exception {
        String data = crypter.encrypt("Test data to be Encrypted by this program");
        System.out.println(data);
    }
}
