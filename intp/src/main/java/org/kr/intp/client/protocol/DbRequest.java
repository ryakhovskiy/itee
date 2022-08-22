package org.kr.intp.client.protocol;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Created bykron 12.06.2014.
 */
public class DbRequest {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static DbRequest createRequestFromString(String request) throws IOException {
        final Map<String, Object> data = (Map)mapper.readValue(request, Map.class);
        DbRequestType msgType = null;
        String projectId = null;
        int version = Integer.MIN_VALUE;
        for (String k : data.keySet()) {
            final String key = k.toLowerCase();
            if (key.equals("command")) {
                final String command = data.get(k).toString().toLowerCase();
                if (command.equals("activate"))
                    msgType = DbRequestType.ACTIVATE;
                else if (command.equals("deactivate"))
                    msgType = DbRequestType.DEACTIVATE;
                else if (command.equals("start"))
                    msgType = DbRequestType.START;
                else if (command.equals("stop"))
                    msgType = DbRequestType.STOP;
                else throw new UnsupportedOperationException("Unsupported request command: " + command);
            }
            if (key.equals("project_id"))
                projectId = data.get(k).toString();
            if (key.equals("version"))
                version = Integer.valueOf(data.get(k).toString());
        }
        if (null == msgType)
            throw new UnsupportedOperationException("Request does not have [command]");
        if (null == projectId)
            throw new UnsupportedOperationException("Request does not have [project_id]");
        //if (msgType == DbRequestType.START && version == Integer.MIN_VALUE)
        //    throw new UnsupportedOperationException("Request [start] does not have [version]");
        return new DbRequest(msgType, projectId, version);
    }

    private final DbRequestType dbRequestCommand;
    private final String projectId;
    private final int version;

    private DbRequest(DbRequestType dbRequestCommand, String projectId) {
        this(dbRequestCommand, projectId, 0);
    }

    private DbRequest(DbRequestType dbRequestCommand, String projectId, int version) {
        this.dbRequestCommand = dbRequestCommand;
        this.projectId = projectId;
        this.version = version;
    }

    public DbRequestType getDbRequestCommand() {
        return dbRequestCommand;
    }

    public String getProjectId() {
        return projectId;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public String toString() {
        if (version < 0)
            return String.format("[%s]: %s", dbRequestCommand.toString(), projectId);
        else
            return String.format("[%s]: %s v. %d", dbRequestCommand.toString(), projectId, version);
    }
}
