package org.kr.itee.perfmon.ws.pojo;

public class MessagePOJO {
    private String heading;
    private String content;
    private String recipients;

    public MessagePOJO() {
    }

    public MessagePOJO(String recipients, String heading, String content) {
        this.heading = heading;
        this.content = content;
        this.recipients = recipients;
    }

    public String getHeading() {
        return heading;
    }

    public void setHeading(String heading) {
        this.heading = heading;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getRecipients() {
        return recipients;
    }

    public void setRecipients(String recipients) {
        this.recipients = recipients;
    }
}
