package PayTM.WeblogChallenge;
import java.io.Serializable;

public class LogEntry implements Serializable {
	private long timestamp;
	private String clientIP;
	private int lbCode;
	private int backendCode;
	private String requestURL;
	LogEntry(long timestamp, String clientIP, String requestURL, int lbCode, int backendCode) {
		this.timestamp = timestamp;
		this.clientIP = clientIP;
		this.lbCode = lbCode;
		this.backendCode = backendCode;
		this.requestURL = requestURL;
	}
	public int getBackendCode() {
		return backendCode;
	}
	public int getLbCode() {
		return lbCode;
	}
	public String getClientIP() {
		return clientIP;
	}
	public String getRequestURL() {
		return requestURL;
	}
	public long getTimestamp() {
		return timestamp;
	}
}