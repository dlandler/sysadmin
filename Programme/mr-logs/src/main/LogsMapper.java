package main;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogsMapper extends Mapper<Text, Text, Text, Text> {

    private String ip_pattern;
    private String date_pattern;
    private String time_pattern;
    private String request_pattern;
    private String statuscode_pattern;
    private String referer_pattern;
    private String browser_pattern;

    private Pattern compiledIpPattern;
    private Pattern compiledDatePattern;
    private Pattern compiledTimePattern;
    private Pattern compiledRequestPattern;
    private Pattern compiledStatuscodePattern;
    private Pattern compiledRefererPattern;
    private Pattern compiledBrowserPattern;

    private Matcher ip_matcher;
    private Matcher date_matcher;
    private Matcher time_matcher;
    private Matcher request_matcher;
    private Matcher statuscode_matcher;
    private Matcher referer_matcher;
    private Matcher browser_matcher;

    public LogsMapper() {
    }

    public void setup(Context context) {
	ip_pattern = "\\d{1,3}(\\.\\d{1,3}){3}";
	date_pattern = "\\d{2}/[a-zA-Z]{3}/\\d{4}";
	time_pattern = "\\d{2}(:\\d{2}){2}";
	request_pattern = " (/[^\\s]+)+ ";
	statuscode_pattern = "\" (\\d{3}) ";
	referer_pattern = " \"([a-z]{1}[^\"]+)";
	browser_pattern = " \"([A-Z][a-z][^\\s]+)";

	compiledIpPattern = Pattern.compile(ip_pattern);
	compiledDatePattern = Pattern.compile(date_pattern);
	compiledTimePattern = Pattern.compile(time_pattern);
	compiledRequestPattern = Pattern.compile(request_pattern);
	compiledStatuscodePattern = Pattern.compile(statuscode_pattern);
	compiledRefererPattern = Pattern.compile(referer_pattern);
	compiledBrowserPattern = Pattern.compile(browser_pattern);
    }

    public void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
	    throws IOException, InterruptedException {

	String sCurrentLine = key.toString();
	String sValues = null;

	createMatcher(sCurrentLine);

	if (ip_matcher.find()) {
	    key = new Text(ip_matcher.group());
	}

	if (date_matcher.find()) {
	    sValues = date_matcher.group() + ";";
	}

	if (time_matcher.find()) {
	    sValues += time_matcher.group() + ";";
	}

	if (request_matcher.find()) {
	    sValues += request_matcher.group(1) + ";";
	}

	if (statuscode_matcher.find()) {
	    sValues += statuscode_matcher.group(1) + ";";
	}

	if (referer_matcher.find()) {
	    sValues += referer_matcher.group(1) + ";";
	}

	if (browser_matcher.find()) {
	    sValues += browser_matcher.group(1);
	}
	value = new Text(sValues);

	context.write(key, value);
    }

    private void createMatcher(String sCurrentLine) {
	ip_matcher = compiledIpPattern.matcher(sCurrentLine);
	date_matcher = compiledDatePattern.matcher(sCurrentLine);
	time_matcher = compiledTimePattern.matcher(sCurrentLine);
	request_matcher = compiledRequestPattern.matcher(sCurrentLine);
	statuscode_matcher = compiledStatuscodePattern.matcher(sCurrentLine);
	referer_matcher = compiledRefererPattern.matcher(sCurrentLine);
	browser_matcher = compiledBrowserPattern.matcher(sCurrentLine);
    }
}
