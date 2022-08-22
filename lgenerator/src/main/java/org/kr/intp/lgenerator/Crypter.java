package org.kr.intp.lgenerator;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * Created by kr on 01.04.2014.
 */
public final class Crypter {

    static Crypter newInstance() {
        return new Crypter();
    }

    private final Cipher encryptor;
    private final Cipher decryptor;

    private Crypter() {
        try {
            int[] arr = getData();
            byte[] bkey = new byte[arr.length];
            for (int i = 0; i < bkey.length; i++)
                bkey[i] = (byte)arr[i];
            SecretKeySpec key = new SecretKeySpec(bkey, "AES");
            this.encryptor = Cipher.getInstance("AES");
            this.encryptor.init(Cipher.ENCRYPT_MODE, key);
            this.decryptor = Cipher.getInstance("AES");
            this.decryptor.init(Cipher.DECRYPT_MODE, key);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw new RuntimeException();
        }
    }

    public byte[] encrypt(byte[] data) {
        try {
            return encryptor.doFinal(data);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw new RuntimeException();
        }
    }

    public byte[] decrypt(byte[] data) {
        try {
            return decryptor.doFinal(data);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw new RuntimeException();
        }
    }

    private int[] getData() {
        int[] data = new int[] { 196, 55, 196, 50, 138, 51, 110, 85, 320, 50, 96, 51, 51, 49, 192, 48 };
        for (int i = 0; i < data.length; i++) {
            if (i == 0)
                data[i] = data[i] >> 2;
            else if (i % 2 == 0)
                data[i] = data[i] >> i % 3;
            else
                data[i] = data[i] ^ (i % 3);
        }
        return data;
    }

}
