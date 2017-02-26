package PayTM.WeblogChallenge;

import org.junit.Test;
import PayTM.WeblogChallenge.App;
import org.junit.Assert;
import org.junit.Before;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AppTest {
	private transient SparkSession spark = null;
	
    @Before
    public void setup() {
    	Logger.getLogger("org").setLevel(Level.OFF);
    	Logger.getLogger("akka").setLevel(Level.OFF);
    	SparkConf sparkConf = new SparkConf()
                .setMaster("local[4]");
    	
    	spark = SparkSession
        		.builder()
        		.config(sparkConf)
        		.appName("Weblog Challenge")
        		.getOrCreate();
    	
    }
    
    @Test
    public void testAll() {
    	Dataset<Row> logEntryDF = App.init(spark, "src/test/resources/small.txt");
    	List<Row> arrResult = logEntryDF.collectAsList();
    	Assert.assertEquals(arrResult.size(), 7);
    	logEntryDF.show();
    	logEntryDF.createOrReplaceTempView("LogEntryView");
    	
    	Dataset<Row> newSessionDF = App.createNewSessionColDF(spark, "LogEntryView", "900000");
    	newSessionDF.show();
    	arrResult = newSessionDF.collectAsList();
    	Assert.assertEquals(arrResult.get(0).getInt(3), 1);
    	Assert.assertEquals(arrResult.get(1).getInt(3), 1);
    	Assert.assertEquals(arrResult.get(2).getInt(3), 1);
    	Assert.assertEquals(arrResult.get(3).getInt(3), 1);
    	Assert.assertEquals(arrResult.get(4).getInt(3), 0);
    	Assert.assertEquals(arrResult.get(5).getInt(3), 1);
    	Assert.assertEquals(arrResult.get(6).getInt(3), 0);
    	
    	newSessionDF.createOrReplaceTempView("NewSessionLogEntryView");
    	Dataset<Row> sessionizedDF = App.createSessionIDColDF(spark, "NewSessionLogEntryView");
    	arrResult = sessionizedDF.collectAsList();
    	sessionizedDF.show();
    	Assert.assertEquals(arrResult.get(5).getLong(1), 2);
    	Assert.assertEquals(arrResult.get(6).getLong(1), 2);
    	
    	sessionizedDF.createOrReplaceTempView("SessionizedLogEntryView");
    	Dataset<Row> sessionTimeDF = App.findDurationPerSessionDF(spark, "SessionizedLogEntryView");
    	arrResult = sessionTimeDF.collectAsList();
    	Assert.assertEquals(arrResult.size(), 5);
    	sessionTimeDF.show();
    	
    	sessionTimeDF.createOrReplaceTempView("SessionTimeLogEntryView");
    	Dataset<Row> avgSessionTimeDF = App.findAvgSessionTimeDF(spark, "SessionTimeLogEntryView");
    	avgSessionTimeDF.show();
    	arrResult = avgSessionTimeDF.collectAsList();
    	Assert.assertEquals(arrResult.size(), 1);
    	
    	Dataset<Row> uniqueVisitsDF = App.findUniqueVisitsPerSessionDF(spark, "SessionizedLogEntryView");
    	uniqueVisitsDF.show();
    	arrResult = uniqueVisitsDF.collectAsList();
    	Assert.assertEquals(arrResult.get(0).getLong(2), 1);
    	Assert.assertEquals(arrResult.get(1).getLong(2), 1);
    	Assert.assertEquals(arrResult.get(2).getLong(2), 1);
    	Assert.assertEquals(arrResult.get(3).getLong(2), 2);
    	Assert.assertEquals(arrResult.get(4).getLong(2), 1);
    	
    	Dataset<Row> mostEngagedDF = App.findMostEngagedUsersDF(spark, "SessionTimeLogEntryView");
    	mostEngagedDF.show();
    	arrResult = mostEngagedDF.collectAsList();
    	Assert.assertEquals(arrResult.size(), 4);
    	Assert.assertTrue(arrResult.get(2).getLong(1) > 0);
    }
}
