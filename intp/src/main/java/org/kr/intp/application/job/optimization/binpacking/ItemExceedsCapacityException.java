package org.kr.intp.application.job.optimization.binpacking;

/**
 */
public class ItemExceedsCapacityException extends Exception{

     public ItemExceedsCapacityException(String message){
         super(message);
     }
}
