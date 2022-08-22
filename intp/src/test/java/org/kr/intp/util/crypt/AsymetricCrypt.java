package org.kr.intp.util.crypt;

import javax.crypto.Cipher;
import java.io.ObjectInputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;

/**
 * Created by kr on 04.02.14.
 */
public class AsymetricCrypt {


    /**
     * String to hold name of the encryption algorithm.
     */
    public static final String ALGORITHM = "RSA";

    /**
     * Encrypt the plain text using public key.
     *
     * @param text
     *          : original plain text
     * @param key
     *          :The public key
     * @return Encrypted text
     * @throws java.lang.Exception
     */
    public static byte[] encrypt(String text, PublicKey key) {
        byte[] cipherText = null;
        try {
            // get an RSA cipher object and print the provider
            final Cipher cipher = Cipher.getInstance(ALGORITHM);
            // encrypt the plain text using the public key
            cipher.init(Cipher.ENCRYPT_MODE, key);
            cipherText = cipher.doFinal(text.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cipherText;
    }

    /**
     * Decrypt text using private key.
     *
     * @param text
     *          :encrypted text
     * @param key
     *          :The private key
     * @return plain text
     * @throws java.lang.Exception
     */
    public static String decrypt(byte[] text, PrivateKey key) {
        byte[] dectyptedText = null;
        try {
            // get an RSA cipher object and print the provider
            final Cipher cipher = Cipher.getInstance(ALGORITHM);

            // decrypt the text using the private key
            cipher.init(Cipher.DECRYPT_MODE, key);
            dectyptedText = cipher.doFinal(text);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return new String(dectyptedText);
    }

    /**
     * Test the EncryptionUtil
     */
    public static void main(String[] args) {

        try {

            //generate keys using RSA algorithm
            final KeyPairGenerator keyGen = KeyPairGenerator.getInstance(ALGORITHM);
            keyGen.initialize(1024);
            final KeyPair key = keyGen.generateKeyPair();

            final String originalText = "Text to be encrypted ";
            ObjectInputStream inputStream = null;

            // Encrypt the string using the public key
            final PublicKey publicKey = key.getPublic();
            final byte[] cipherText = encrypt(originalText, publicKey);

            // Decrypt the cipher text using the private key.
            final PrivateKey privateKey = key.getPrivate();
            final String plainText = decrypt(cipherText, privateKey);

            // Printing the Original, Encrypted and Decrypted Text
            System.out.println("Original Text: " + originalText);
            System.out.println("Encrypted Text: " + new String(cipherText));
            System.out.println("Decrypted Text: " + plainText);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
