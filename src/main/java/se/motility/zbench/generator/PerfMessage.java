package se.motility.zbench.generator;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PerfMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private long timestamp;
    private long sequence;
    private int i;
    private boolean b;
    private String id;
    private String s1;
    private String s2;
    private String s3;
    private String s4;

    public PerfMessage() {
        // required by jackson
    }

    public PerfMessage(long timestamp, long sequence, int i, boolean b, String id, String s1, String s2, String s3, String s4) {
        this.timestamp = timestamp;
        this.sequence = sequence;
        this.i = i;
        this.b = b;
        this.id = id;
        this.s1 = s1;
        this.s2 = s2;
        this.s3 = s3;
        this.s4 = s4;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public int getI() {
        return i;
    }

    public void setI(int i) {
        this.i = i;
    }

    public boolean isB() {
        return b;
    }

    public void setB(boolean b) {
        this.b = b;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getS1() {
        return s1;
    }

    public void setS1(String s1) {
        this.s1 = s1;
    }

    public String getS2() {
        return s2;
    }

    public void setS2(String s2) {
        this.s2 = s2;
    }

    public String getS3() {
        return s3;
    }

    public void setS3(String s3) {
        this.s3 = s3;
    }

    public String getS4() {
        return s4;
    }

    public void setS4(String s4) {
        this.s4 = s4;
    }

}
