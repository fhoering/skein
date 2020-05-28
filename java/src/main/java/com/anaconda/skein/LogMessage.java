package com.anaconda.skein;

public class LogMessage {
    private String key;
    private String data;

    public LogMessage(String key, String data) {
        this.key = key;
        this.data = data;
    }

    public String getKey() {
        return key;
    }

    public String getData() {
        return data;
    }
}
