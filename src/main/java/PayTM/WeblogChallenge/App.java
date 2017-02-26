package PayTM.WeblogChallenge;

import java.io.Serializable;
import java.text.SimpleDateFormat;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App 
{
	private final static int TIMESTAMP_INDEX = 0;
	private final static int CLIENT_INDEX = 2;
	private final static int LB_STATUS_CODE_INDEX = 7;
	private final static int BACKEND_STATUS_CODE_INDEX = 8;
	private final static int REQUEST_URL_INDEX = 12;

    public static void main( String[] args ) throws ClassNotFoundException
    {
        if (args.length != 1) {
        	System.exit(-1);
        }
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        String logFile = args[0];
        SparkSession spark = SparkSession
        		.builder()
        		.appName("Weblog Challenge")
        		.getOrCreate();
        
        JavaRDD<String> logEntryStringRDD = spark.read().textFile(logFile).javaRDD();

        // ETL
        JavaRDD<LogEntry> logEntryRDD = logEntryStringRDD.map(new Function<String, LogEntry>() {
			public LogEntry call(String line) throws Exception {
				String[] attrs = line.split(" ");
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
				long timestamp = formatter.parse(attrs[TIMESTAMP_INDEX]).getTime();
				String clientIP = attrs[CLIENT_INDEX].split(":")[0];
				int lbCode = Integer.parseInt(attrs[LB_STATUS_CODE_INDEX]);
				int backendCode = Integer.parseInt(attrs[BACKEND_STATUS_CODE_INDEX]);
				String requestURL = attrs[REQUEST_URL_INDEX];
				LogEntry log = new LogEntry(timestamp, clientIP, requestURL, lbCode, backendCode); 
				return log;
			}
        }).filter(new Function<LogEntry, Boolean>() {
			public Boolean call(LogEntry entry) throws Exception {
				// Assumption 1 - non-200 code requests are not considered as valid hits
				// Assumption 2 - static resource requests are not page visits/valid hits
				// Assumption 3 - all static resources are in the path /offer/*
				if (entry.getBackendCode() != 200 || entry.getLbCode() != 200) {
					return false;
				}
				if (entry.getRequestURL().matches("(.*)/offer/(.*)")) {
					return false;
				}
				return true;
			}  	
        });
        Dataset<Row> logEntryDF = spark.createDataFrame(logEntryRDD, LogEntry.class);
        // Register logEntryDF as a temporary view
        logEntryDF.createOrReplaceTempView("LogEntry");

        // Assume a session duration is 15 mins (15 * 60 * 1000 = 900000)
        // newSession [boolean] = LAG(timestamp) = null or timestamp - LAG(timestamp) > 900000 THEN 1 ELSE 0
        String newSessionSelection = "CASE WHEN (LAG(timestamp) OVER (PARTITION BY clientIP ORDER BY timestamp) IS NULL) OR (timestamp - LAG(timestamp) OVER (PARTITION BY clientIP ORDER BY timestamp) > 900000) THEN 1 ELSE 0 END AS newSession";
        String newSessionQuery = "SELECT timestamp, clientIP, requestURL, " + newSessionSelection + " FROM LogEntry";
        Dataset<Row> newSessionDF = spark.sql(newSessionQuery);
        
        // Register newSessionDF as a temporary view
        newSessionDF.createOrReplaceTempView("NewSessionLogEntry");
        
        // Q1 - Sessionize:
        // sessionID = cumulative SUM(newSession)
        // clientIP + sessionID can uniquely identify a session.
        String sessionizationQuery = "SELECT clientIP, SUM(newSession) OVER (PARTITION BY clientIP ORDER BY timestamp) AS sessionID, requestURL, timestamp FROM NewSessionLogEntry";
        Dataset<Row> sessionizationDF = spark.sql(sessionizationQuery);
        // Register sessionizationDF as a temporary view
        sessionizationDF.createOrReplaceTempView("SessionizedLogEntry");
        String sessionTimeQuery = "SELECT clientIP, sessionID, (MAX(timestamp) - MIN(timestamp)) as sessionTime FROM SessionizedLogEntry GROUP BY sessionID, clientIP";
        Dataset<Row> sessionTimeDF = spark.sql(sessionTimeQuery);
        
        // Register sessionTimeDiffDF as a temporary view
        sessionTimeDF.createOrReplaceTempView("SessionTimeLogEntry");
        System.out.println("Q1 - Sessionize:");
        sessionTimeDF.show(100);
        
        // Q2 - Avg Session Time:
        System.out.println("Q2 - Avg Session Time:");
        String avgSessionTimeQuery = "SELECT AVG(sessionTime) FROM SessionTimeLogEntry";
        spark.sql(avgSessionTimeQuery).show();
        
        // Q3 - Unique URL Visits per Session
        System.out.println("Q3 - Unique URL Visits per Session:");
        String uniqueVisitPerSessionQuery = "SELECT clientIP, sessionID, COUNT(DISTINCT requestURL) AS uniqueVisits FROM SessionizedLogEntry GROUP BY sessionID, clientIP";
        Dataset<Row> uniqueVisitPerSessionDF = spark.sql(uniqueVisitPerSessionQuery);
        uniqueVisitPerSessionDF.show(100);
        
        // Q4 - The Most Engaged Users
        System.out.println("Q4 - The Most Engaged Users:");
        String mostEngagedUsersQuery = "SELECT clientIP, SUM(SessionTime) AS engagedTime FROM SessionTimeLogEntry GROUP BY clientIP SORT BY engagedTime DESC";
        Dataset<Row> mostEngagedUsersDF = spark.sql(mostEngagedUsersQuery);
        mostEngagedUsersDF.show(100);
    }
}
