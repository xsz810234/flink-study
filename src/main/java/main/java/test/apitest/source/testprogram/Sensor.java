package main.java.test.apitest.source.testprogram;

public class Sensor {
    private String sensorId;
    private double temprature;
    private long timestamp;

    public Sensor(){

    }

    public Sensor(String sensorId, long timestamp, double temprature){
        this.sensorId = sensorId;
        this.temprature = temprature;
        this.timestamp = timestamp;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public double getTemprature() {
        return temprature;
    }

    public void setTemprature(double temprature) {
        this.temprature = temprature;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Sensor{" +
                "sensorId='" + sensorId + '\'' +
                ", temprature=" + temprature +
                ", timestamp=" + timestamp +
                '}';
    }
}
