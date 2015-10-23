package com.mireau.homeAutomation.timeseries;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.Thread.State;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.mireau.homeAutomation.timeseries.ArchiveDataSerie.ArchivePoint;
import com.mireau.homeAutomation.timeseries.ArchiveDataSerie.Type;
import com.mireau.homeAutomation.timeseries.RawDataSerie.Entry;


public class Test {

	static Logger logger;
	static SimpleDateFormat sdf = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss");
	
	static TimeSeriesDB ts;
	
	public static void main(String[] params) {
		InputStream is = null;
		Scanner scanner = new Scanner(System.in);
		
		try {
			/*
			 * Shutdown hook
			 */
			Runtime.getRuntime().addShutdownHook(new Thread(){
				@Override
				public void run() {
					logger.log(Level.INFO, "shutdown hook");
					try {
						if (ts != null)
							ts.close();
					} catch (IOException e) {
						logger.log(Level.SEVERE, e.getMessage(), e);
					}
				}
			});
			
			/*
			 * Initialisation du LogManager
			 */
			File file = new File("logging.properties");
			if (file.exists()) {
				is = new FileInputStream(file);
			} else {
				// conf par défaut
				is = Test.class.getClassLoader().getResourceAsStream(
						Test.class.getPackage().getName().replaceAll("\\.", "/") + "/logging.properties");
			}
			LogManager.getLogManager().readConfiguration(is);
			logger = Logger.getLogger(Test.class.getName());
			is.close();
			
			/*
			 * Console
			 */
			prompt(null);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				String[] terms = line.split("[ \t]+");
				if(terms.length == 0) continue;
				
				String verb = terms[0];
				
				if ("open".equalsIgnoreCase(verb)) {
					if(ts != null && !ts.isClosed())
						ts.close();
					if(terms.length < 2){
						System.out.println("usage: open <timeserie-name>");
						continue;
					}
					String dbName = terms[1];
					ts = new TimeSeriesDB(dbName,".");
				}
				else if ("close".equalsIgnoreCase(verb)) {
					ts.close();
				}
				else if ("build".equalsIgnoreCase(verb)) {
					ts.buildArchive(15*60, Type.AVERAGE);
				}
				else if ("put".equalsIgnoreCase(verb)) {
					if(terms.length < 2){
						System.out.println("usage: put <float value>");
						continue;
					}
					try{
						float value = Float.parseFloat(terms[1]);
						ts.post((new Date()).getTime()/1000, value);
					}
					catch(NumberFormatException e){
						System.out.println("nombre malformé");
						continue;
					}
				}
				else if ("last".equalsIgnoreCase(verb)) {
					Entry e = ts.getLast();
					if(e!=null){
						System.out.println(sdf.format(e.timestamp*1000)+" : "+e.value);
					}
					else{
						System.out.println("no value");
					}
				}
				else if ("get".equalsIgnoreCase(verb)) {
					List<ArchivePoint> list = ts.getArchive(15*60, Type.AVERAGE).getPoints(null,null);
					for (ArchivePoint point : list) {
						System.out.println(sdf.format(new Date(point.timestamp*1000))+" : "+point.value);
					}
				}
				else if ("quit".equalsIgnoreCase(verb) || "exit".equalsIgnoreCase(verb)) {
					break;
				}
				else if ("dump".equalsIgnoreCase(verb)) {
					printThreadDump(System.out);
					prompt(null);
				}
				else if ("".equalsIgnoreCase(verb)) {
					prompt(null);
				}
				else {
					prompt("unknown command : " + verb);
				}
			}
		} catch (IOException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			System.exit(1);
		} catch (ArchiveInitException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			System.exit(2);
		} catch (Throwable e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			System.exit(3);
		}
		finally {
			scanner.close();
		}
		
	}

	static void prompt(String msg) {
		if (msg != null && !msg.trim().equals(""))
			System.out.println(msg);
		System.out.print(">");
	}
	
	public static void printThreadDump(PrintStream ps) {
		final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
		final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
		for (ThreadInfo threadInfo : threadInfos) {
			ps.print('"');
			ps.print(threadInfo.getThreadName());
			ps.print("\" ");
			final Thread.State state = threadInfo.getThreadState();
			ps.print("\n   java.lang.Thread.State: ");
			ps.print(state);
			if(state.equals(State.WAITING)){
				ps.print(" (");
				ps.print(threadInfo.getWaitedTime());
				ps.print("ms by ");
				ps.print(threadInfo.getLockOwnerName());
				ps.print(" : ");
				ps.print(threadInfo.getLockName());
				ps.print(")");
			}
			//Pile
			final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
			for (final StackTraceElement stackTraceElement : stackTraceElements) {
				ps.print("\n        at ");
				ps.print(stackTraceElement);
			}
			ps.print("\n\n");
		}
	}
}
