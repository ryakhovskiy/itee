package org.kr.intp.application.manager;

public interface IProjectManager {

    public void start(String projectId) throws Exception;
    public void stop(String projectId) throws Exception;
    public void deactivate(String projectId) throws Exception;
    public void activate(String projectId, int version) throws Exception;

}
