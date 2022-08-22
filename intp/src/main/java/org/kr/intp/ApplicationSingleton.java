package org.kr.intp;

import javax.annotation.PostConstruct;
import javax.ejb.Singleton;
import javax.ejb.Startup;

/**
 */
@Startup
@Singleton
public class ApplicationSingleton {

    @PostConstruct
    public void init(){
        System.out.println("Init method called");
    }

    public void sayHello(){
        System.out.println("Hello ya");
    }
}
