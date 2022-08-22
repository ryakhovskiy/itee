package org.kr.intp.util.crypt;

import java.io.*;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;

/**
 * Created by kr on 04.02.14.
 */
public class DigitalSignCrypt {

    private static final String PRIVATE = "c:\\proj\\dsa\\private.key";
    private static final String PUBLIC = "c:\\proj\\dsa\\public.key";

    private static boolean areKeysExists() {
        File priv = new File(PRIVATE);
        File pub = new File(PUBLIC);
        return  (priv.exists() && pub.exists());
    }

    private static void generateKeys() throws NoSuchProviderException, NoSuchAlgorithmException, IOException {
        System.out.printf("generating keys...%n");
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("DSA", "SUN");
        KeyPair pair = keyGen.generateKeyPair();
        saveKey(PRIVATE, pair.getPrivate());
        saveKey(PUBLIC, pair.getPublic());
    }

    private static void saveKey(String name, Key key) throws IOException {
        ObjectOutputStream stream = null;
        try {
            stream = new ObjectOutputStream(new FileOutputStream(name));
            stream.writeObject(key);
        } finally {
            if (null != stream)
                stream.close();
        }
    }

    private static Key readKey(String file) throws IOException, ClassNotFoundException {
        ObjectInputStream stream = null;
        try {
            stream = new ObjectInputStream(new FileInputStream(file));
            return  (Key)stream.readObject();
        } finally {
            if (null != stream)
                stream.close();
        }
    }

    public static void main(String... args) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, NoSuchProviderException, InvalidKeyException, SignatureException, ClassNotFoundException {
        if (!areKeysExists())
            generateKeys();
        PublicKey publicKey = (PublicKey)readKey(PUBLIC);
        PrivateKey privateKey = (PrivateKey)readKey(PRIVATE);

        String data = "data to be signed";

        byte[] realSig = makeDsa(privateKey, data);
        boolean verified = ensureDsa(publicKey, realSig, data);

        System.out.printf("public key: %s%n", Arrays.toString(publicKey.getEncoded()));
        System.out.printf("private key: %s%n", Arrays.toString(privateKey.getEncoded()));
        System.out.printf("data: %s%n", data);
        System.out.printf("real signature: %s%n", Arrays.toString(realSig));
        System.out.printf("verified: %b%n", verified);
    }

    private static byte[] makeDsa(PrivateKey privateKey, String data) throws NoSuchProviderException, NoSuchAlgorithmException, InvalidKeyException, UnsupportedEncodingException, SignatureException {
        Signature dsa = Signature.getInstance("SHA1withDSA", "SUN");
        dsa.initSign(privateKey);
        dsa.update(data.getBytes("UTF8"));
        return dsa.sign();

    }

    public static boolean ensureDsa(PublicKey key, byte[] sign, String data) throws NoSuchProviderException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException, UnsupportedEncodingException {
        X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(key.getEncoded());
        KeyFactory keyFactory = KeyFactory.getInstance("DSA", "SUN");
        PublicKey pubKey = keyFactory.generatePublic(pubKeySpec);
        Signature dsa = Signature.getInstance("SHA1withDSA", "SUN");
        dsa.initVerify(pubKey);
        dsa.update(data.getBytes("UTF8"));
        return dsa.verify(sign);
    }
}
