package com.mireau.timeseries;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.Thread.State;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.mireau.timeseries.ArchiveTimeSerie.Type;
import com.mireau.timeseries.RawTimeSerie.Entry;


public class Test {

	static Logger logger;
	static SimpleDateFormat sdf = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss");
	
	static TimeSerie ts;
	
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
					} catch (IOException | ArchiveInitException e) {
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
				is = Test.class.getClassLoader().getResourceAsStream(Test.class.getPackage().getName().replaceAll("\\.", "/") + "/logging.properties");
				if(is==null) is = Test.class.getClassLoader().getResourceAsStream("logging.properties");
			}
			if(is!=null){
				LogManager.getLogManager().readConfiguration(is);
				logger = Logger.getLogger(Test.class.getName());
				is.close();
			}
			
			new TimeSeriesDB(new File("."));
			
			/*
			 * Console
			 */
			prompt(null);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				String[] terms = line.split("[ \t]+");
				if(terms.length == 0) continue;
				
				String verb = terms[0];
				
				if ("help".equalsIgnoreCase(verb)) {
					printUsage();
				}
				else if ("open".equalsIgnoreCase(verb)) {
					if(ts != null && !ts.isClosed())
						ts.close();
					if(terms.length < 2){
						System.out.println("usage: open <timeserie-name>");
						continue;
					}
					String dbName = terms[1];
					ts = new TimeSerie(dbName,".");
					
					printStatus(ts);
				}
				else if ("close".equalsIgnoreCase(verb)) {
					ts.close();
					ts = null;
				}
				else if ("status".equalsIgnoreCase(verb)) {
					printStatus(ts);
				}
				else if ("build".equalsIgnoreCase(verb)) {
					ArchiveTimeSerie archive = ts.getArchive(15*60, Type.AVERAGE);
					ts.buildArchive(archive);
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
					
					if(terms.length < 3){
						System.out.println("usage: get <step> <periode> [start timestamp]");
						continue;
					}
					try{
						int step = Integer.parseInt(terms[1]);
						String periodStr = terms[2];
						Long start = (terms.length>3 ? Long.parseLong(terms[3]) : null);
						Type type = Type.AVERAGE;
						
						
						
						List<ArchivePoint> points = ts.select(step,type,start,periodStr);
						
						NumberFormat numberFormater = NumberFormat.getInstance();
						numberFormater.setGroupingUsed(false);
						numberFormater.setRoundingMode(RoundingMode.HALF_DOWN);
						numberFormater.setMinimumFractionDigits(0);
						numberFormater.setMaximumFractionDigits(5);
						numberFormater.setMinimumIntegerDigits(0);
						numberFormater.setMaximumIntegerDigits(10);
						DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss");
						
						ts.exportCSV(points, System.out, dateFormat, numberFormater);
					}
					catch(NumberFormatException e){
						System.out.println("arguments malformé");
						continue;
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
	
	public static void printUsage() throws IOException{
		System.out.println("commands :");
		System.out.println("   help  : this message");
		System.out.println("   open  : open time serie by id");
		System.out.println("   close : close time serie");
		System.out.println("   status: status of this vitual machine");
		System.out.println("   build : build 15mn average archive");
		System.out.println("   put   : put value in the timeserie");
		System.out.println("   last  : get last value");
		System.out.println("   get   : request archive points");
		System.out.println("   ");
	}
	
	public static void printStatus(TimeSerie ts) throws IOException{
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/Y-HH:mm");
		if(ts == null){
			System.out.println("no opened time serie");
			return;
		}
		System.out.println("raw data file : "+ts.rawDS.getFile().getName());
	
		if(ts.archives.isEmpty()){
			System.out.println("no archive");
		}	
		else{
			System.out.println("archives :");
			for (ArchiveTimeSerie archive : ts.archives) {
				if(archive == null) continue;
				System.out.println("   "+archive.archiveFile+" type:"+archive.type()+" step:"+archive.step/60+"min debut:"+sdf.format(new Date(archive.t0*1000))+" len="+archive.archiveFile.length()+" last="+archive.lastTimestamp);
				if(archive instanceof AverageArchiveDataSerie){
					AverageArchiveDataSerie a = (AverageArchiveDataSerie)archive;
					System.out.println("      stepTimestamp:"+a.stepTimestamp+" stepNb:"+a.stepNb+" stepLast:"+a.stepLast+" stepMin:"+a.stepMin+" stepMax:"+a.stepMax+" stepSum:"+a.stepSum);
				}
				List<ArchivePoint> l = archive.getLastPoints(3);
				for (ArchivePoint p : l) {
					System.out.println("     "+sdf.format(new Date(p.timestamp*1000))+" : "+p.value);
				}
			}
		}
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
