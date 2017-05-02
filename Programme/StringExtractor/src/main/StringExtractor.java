package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringExtractor {

    public static String FILENAME = "/Users/Daniel/Documents/example.txt";

    public static void main(String[] args) {

	BufferedReader br = null;
	FileReader fr = null;

	try {

	    fr = new FileReader(FILENAME);
	    br = new BufferedReader(fr);

	    String sCurrentLine;

	    while ((sCurrentLine = br.readLine()) != null) {
		System.out.println(sCurrentLine);

		String ip_pattern = "\\d{1,3}(\\.\\d{1,3}){3}";
		String date_pattern = "\\d{2}/[a-zA-Z]{3}/\\d{4}";
		String time_pattern = "\\d{2}(:\\d{2}){2}";
		String request_pattern = " (/[^\\s]+)+ ";
		String statuscode_pattern = "\" (\\d{3}) ";

		Pattern compiledIpPattern = Pattern.compile(ip_pattern);
		Pattern compiledDatePattern = Pattern.compile(date_pattern);
		Pattern compiledTimePattern = Pattern.compile(time_pattern);
		Pattern compiledRequestPattern = Pattern.compile(request_pattern);
		Pattern compiledStatuscodePattern = Pattern.compile(statuscode_pattern);

		Matcher ip_matcher = compiledIpPattern.matcher(sCurrentLine);
		Matcher date_matcher = compiledDatePattern.matcher(sCurrentLine);
		Matcher time_matcher = compiledTimePattern.matcher(sCurrentLine);
		Matcher request_matcher = compiledRequestPattern.matcher(sCurrentLine);
		Matcher statuscode_matcher = compiledStatuscodePattern.matcher(sCurrentLine);

		if (ip_matcher.find()) {
		    System.out.println("IP: " + ip_matcher.group());
		}

		if (date_matcher.find()) {
		    System.out.println("Date: " + date_matcher.group());
		}

		if (time_matcher.find()) {
		    System.out.println("Time: " + time_matcher.group());
		}

		if (request_matcher.find()) {
		    System.out.println("Request: " + request_matcher.group());
		}

		if (statuscode_matcher.find()) {
		    System.out.println("Statuscode: " + statuscode_matcher.group(1));
		}

	    }

	    br.close();
	    fr.close();

	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

}
