package main.java.test.model;

public class UrlCountPOJO {
    String url;
    Long times;

    Long windowStart;
    Long windowEnd;

    public UrlCountPOJO() {
    }

    public UrlCountPOJO(String url, Long times) {
        this.url = url;
        this.times = times;
    }

    public UrlCountPOJO(String url, Long times, Long windowStart, Long windowEnd) {
        this.url = url;
        this.times = times;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Long windowStart) {
        this.windowStart = windowStart;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getTimes() {
        return times;
    }

    public void setTimes(Long times) {
        this.times = times;
    }

    @Override
    public String toString() {
        return "UrlCountPOJO{" +
                "url='" + url + '\'' +
                ", times=" + times +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
