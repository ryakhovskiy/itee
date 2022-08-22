package org.kr.intp.util.hash;

public class HashingFactory {

    public IHashing createHashing(String algorithm) {
        return new HashingImpl(algorithm.toUpperCase());
    }

    public static HashingFactory newHashingFactory() { return new HashingFactory(); }

    private HashingFactory() {}
}

