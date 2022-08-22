package org.kr.intp.util.hash;

public interface IHashing {

    String getHash(String data) throws HashingException;
    byte[] getHash(byte[] data) throws HashingException;

    boolean check(String data, String hash) throws HashingException;
    boolean check(byte[] data, byte[] hash) throws HashingException;

}

