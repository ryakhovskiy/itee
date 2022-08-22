package org.kr.intp.util.hash;

import java.util.Base64;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 *
 */
public class HashingImpl implements IHashing {

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF16");
    private static final Base64.Encoder base64encoder = Base64.getEncoder();
    //private static final Base64.Decoder base64decoder = Base64.getDecoder();

    private final String algorithm;
    private final Charset charset;

    HashingImpl(String algorithm) {
        this(algorithm, DEFAULT_CHARSET);
    }

    HashingImpl(String algorithm, String encoding) {
        this(algorithm, Charset.forName(encoding));
    }

    HashingImpl(String algorithm, Charset charset) {
        this.algorithm = algorithm;
        this.charset = charset;
    }

    public String getHash(String data) throws HashingException {
        return hash(data);
    }

    public byte[] getHash(byte[] data) throws HashingException {
        return hash(data);
    }

    public boolean check(String data, String hash) throws HashingException {
        final String dhash = hash(data);
        return dhash.equals(hash);
    }

    public boolean check(byte[] data, byte[] hash) throws HashingException {
        final byte[] dhash = hash(data);
        return Arrays.equals(dhash, hash);
    }

    private byte[] hash(byte[] data) throws HashingException {
        try {
            final MessageDigest messageDigest = MessageDigest.getInstance(algorithm);
            messageDigest.update(data);
            return messageDigest.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new HashingException(e);
        }
    }

    private String hash(String data) throws HashingException {
        final byte[] digest = hash(data.getBytes(charset));
        return base64encoder.encodeToString(digest);
    }
}
