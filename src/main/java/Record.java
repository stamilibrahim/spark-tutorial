/**
 * Created by ibrahim on 2/22/18.
 */
public class Record {

     /*
    This class holds call specific information.
    Each object of this class stores a single call data record
     */

    private String caller_msisdn, callee_msisdn, date, time, duration, action_type, caller_imei,
            callee_imei, caller_imsi, callee_imsi, base_station_position, caller_operator_name,
            callee_operator_name, base_station_id, end_location, latitude, longitude;

    // generate event ID
    public String generateEventId(){
        return "BLUERAY_EVENT_" + this.caller_msisdn + this.callee_msisdn + this.date + this.time + this.base_station_id;
    }

    public String getCaller_msisdn() {
        return caller_msisdn;
    }

    public void setCaller_msisdn(String caller_msisdn) {
        this.caller_msisdn = caller_msisdn;
    }

    public String getCallee_msisdn() {
        return callee_msisdn;
    }

    public void setCallee_msisdn(String callee_msisdn) {
        this.callee_msisdn = callee_msisdn;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getAction_type() {
        return action_type;
    }

    public void setAction_type(String action_type) {
        this.action_type = action_type;
    }

    public String getCaller_imei() {
        return caller_imei;
    }

    public void setCaller_imei(String caller_imei) {
        this.caller_imei = caller_imei;
    }

    public String getCallee_imei() {
        return callee_imei;
    }

    public void setCallee_imei(String callee_imei) {
        this.callee_imei = callee_imei;
    }

    public String getCaller_imsi() {
        return caller_imsi;
    }

    public void setCaller_imsi(String caller_imsi) {
        this.caller_imsi = caller_imsi;
    }

    public String getCallee_imsi() {
        return callee_imsi;
    }

    public void setCallee_imsi(String callee_imsi) {
        this.callee_imsi = callee_imsi;
    }

    public String getBase_station_position() {
        return base_station_position;
    }

    public void setBase_station_position(String base_station_position) {
        this.base_station_position = base_station_position;
    }

    public String getCaller_operator_name() {
        return caller_operator_name;
    }

    public void setCaller_operator_name(String caller_operator_name) {
        this.caller_operator_name = caller_operator_name;
    }

    public String getCallee_operator_name() {
        return callee_operator_name;
    }

    public void setCallee_operator_name(String callee_operator_name) {
        this.callee_operator_name = callee_operator_name;
    }

    public String getBase_station_id() {
        return base_station_id;
    }

    public void setBase_station_id(String base_station_id) {
        this.base_station_id = base_station_id;
    }

    public String getEnd_location() {
        return end_location;
    }

    public void setEnd_location(String end_location) {
        this.end_location = end_location;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

}
