package se.motility.zbench.generator;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MetaData implements Serializable {

    private static final long serialVersionUID = 1L;

    private int version;
    private int files;
    private long messages;

    public MetaData() {
        //required by jackson
    }

    public MetaData(int version, int files, long messages) {
        this.version = version;
        this.files = files;
        this.messages = messages;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getFiles() {
        return files;
    }

    public void setFiles(int files) {
        this.files = files;
    }

    public long getMessages() {
        return messages;
    }

    public void setMessages(long messages) {
        this.messages = messages;
    }
}
