package com.AccessTrafficStatistics;

import java.io.Serializable;


//log日志的javaBean
public class AccessLogInfo implements Serializable {

    private static final  long  serivaVersionUID = 1L;
    private long  Timestamp;
    private  long upTraffic;
    private  long downTraffic;

    public AccessLogInfo(long timestamp, long upTraffic, long downTraffic) {
        Timestamp = timestamp;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    public long getTimestamp() {
        return Timestamp;
    }

    public void setTimestamp(long timestamp) {
        Timestamp = timestamp;
    }

    public long getUpTraffic() {
        return upTraffic;
    }

    public void setUpTraffic(long upTraffic) {
        this.upTraffic = upTraffic;
    }

    public long getDownTraffic() {
        return downTraffic;
    }

    public void setDownTraffic(long downTraffic) {
        this.downTraffic = downTraffic;
    }
}