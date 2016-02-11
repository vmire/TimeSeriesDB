package com.mireau.timeseries;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.mireau.timeseries.Archive.Type;
import com.mireau.timeseries.RawData.Entry;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DataSerieTest {
	
	
	static String DIR = "./test-tmp";
	
	static SimpleDateFormat sdf = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss");
	//static Logger logger = Logger.getLogger(DataSerieTest.class.getName());
	
	//@Test(expected = ArithmeticException.class)
	//@Test(timeout = 1000)
	//@ignore
	
	static{
		/*
		 * Initialisation du LogManager
		 */
		InputStream is;
		try {
			is = Test.class.getClassLoader().getResourceAsStream("logging.properties");
			
			LogManager.getLogManager().readConfiguration(is);
			Logger logger = Logger.getLogger(Test.class.getName());
			logger.fine("test");
			is.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		/*
		 * Nettoyage du répertoire
		 */
		File dir = new java.io.File(DIR);
		if(!dir.exists()){
			//Création du répertoire
			dir.mkdirs();
		}
		else{
			//Suppression des fichiers qui gèneraient
			System.out.println("le répertoire existe deja");
			File[] files = dir.listFiles();
			for(int i=0; i<files.length;i++){
				File f = files[i];
				System.out.println("suppression de "+f.getName());
				f.delete();
			}
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		
	}

	//Avant chaque test
	@Before
	public void setUp() throws Exception {
		
	}

	//Après chaque test
	@After
	public void tearDown() throws Exception {
		
	}

	
	
	
	@Test
	public void t01_test() throws IOException, ArchiveInitException {
		TimeSerie ts;
		File rawFile;
		final String TEST_SERIE_NAME = "test-db";
		
		//Création de la TimeSerieDB
		ts = new TimeSerie(TEST_SERIE_NAME,DIR);
		ts.getMeta().setType(Type.AVERAGE);
		rawFile = new File(DIR+"/ts_"+TEST_SERIE_NAME+".rts");
		
		/* 
		 * Archive sur 5 minutes / tests de création et à vide
		 */
		Archive archive5 = ts.createArchive(5*60);
		archive5.setWriteStartegy(Archive.WriteStrategy.CHANGE_STEP);
		Assert.assertEquals(1,ts.archives.size());
		
		File archive5File = new File(DIR+"/ts_"+TEST_SERIE_NAME+"_"+(5*60)+".ats");
		Assert.assertTrue(archive5File.exists());
		//on est censé avoir aucun enregistrement dans l'archive, ni même les données de step en cours
		Assert.assertEquals(Archive.HEADER1_LEN,archive5File.length());
		
		/*
		 * Enregistrements
		 */
		Calendar cal = GregorianCalendar.getInstance();
		cal.set(2015, 11, 07, 00, 10);	// 00:10
		
		
		/*
		 * Premier enregistrement
		 */
		ts.post(cal.getTime(), 1);
		
		//La taille du fichier raw doit correspondre à la taille d'un enregistrement
		int nb = 1;
		Assert.assertEquals(rawFile.length(), RawData.DATA_LEN * nb);
		
		//on est censé n'avoir aucun enregistrement dans l'archive de 5mn
		long expectedLen = Archive.HEADER1_LEN;
		if(archive5.getWriteStartegy() == Archive.WriteStrategy.ALL_POINTS)  expectedLen += archive5.currentStepDataLength(); 
		Assert.assertEquals(expectedLen,archive5File.length());
		
		/*
		 * Deuxième enregistrement
		 */
		cal.add(Calendar.MINUTE, 1);	//00:11		T5+1
		nb++;
		ts.post(cal.getTime(), 2);
		Assert.assertEquals(rawFile.length(), RawData.DATA_LEN * nb);
		
		//on est censé n'avoir aucun enregistrement dans l'archive de 5mn
		Assert.assertEquals(expectedLen,archive5File.length());
				
		/*
		 * Troisième enregistrement
		 */
		cal.add(Calendar.MINUTE, 5);	//00:16		T5+6
		nb++;
		ts.post(cal.getTime(), 3);
		Assert.assertEquals(rawFile.length(), RawData.DATA_LEN * nb);
		
		//on est censé avoir 1 enregistrement dans l'archive de 5mn, 
		assertNbInArchiveFile(1,archive5,archive5File);	
	
		/*
		 * 4° enregistrement
		 */
		cal.add(Calendar.MINUTE, 10);	//00:26 	T5+16
		nb++;
		ts.post(cal.getTime(), 4);
		Assert.assertEquals(rawFile.length(), RawData.DATA_LEN * nb);
		
		//on est censé avoir 2 enregistrement dans l'archive de 5mn
		assertNbInArchiveFile(2,archive5,archive5File);
		
		/*
		 * 5° enregistrement
		 */
		cal.add(Calendar.MINUTE, 5);	//00:31		T5+21
		nb++;
		ts.post(cal.getTime(), 5);
		Assert.assertEquals(rawFile.length(), RawData.DATA_LEN * nb);
		
		//on est censé avoir 4 enregistrement dans l'archive de 5mn
		assertNbInArchiveFile(4,archive5,archive5File);
		
		/*
		 * construction d'une archive 15 mn. Elle doit récupérer les valeurs brutes déjà enregistrées
		 */
		Archive archive15 = ts.createArchive(15*60);
		File archive15File = new File(DIR+"/ts_"+TEST_SERIE_NAME+"_"+(15*60)+".ats");
		Assert.assertTrue(archive15File.exists());
		
		//on est censé avoir 2 enregistrements dans l'archive de 15mn 
		assertNbInArchiveFile(2,archive15, archive15File);
		
		//On vérifie la date de début de l'archive reconstruite
		RandomAccessFile raf = new RandomAccessFile(archive15File, "r");
		raf.seek(8);
		long startTimestamp = raf.readLong();
		raf.close();
		Calendar startCal = GregorianCalendar.getInstance();
		startCal.set(2015, 11, 07, 00, 00, 00);	// datetime théorique de début	
		Assert.assertEquals(startCal.getTimeInMillis()/1000,startTimestamp);
		
		//Vérification de la date en mémoire dans l'objet
		Assert.assertEquals(startTimestamp,(long)archive15.startTimestamp);
		
		/*
		 * Requete
		 */
		Entry e = ts.getLast();
		Assert.assertEquals(5,e.value,0.0000001);
		Assert.assertEquals(cal.getTimeInMillis()/1000,e.timestamp);
		
		/*
		 * On termine : fermeture du TimeSerieDB
		 */
		if (ts != null)
			ts.close();
	}
	
	
	
	
	
	
	/**
	 * Test d'une serie AbsCounter
	 * @throws IOException
	 * @throws ArchiveInitException
	 */
	@Test
	public void t02_test() throws IOException, ArchiveInitException {
		TimeSerie ts;
		File rawFile;
		
		final String name = "test-counter";
		
		//Création de la TimeSerieDB
		ts = new TimeSerie(name,DIR);
		ts.getMeta().setType(Type.ABS_COUNTER);
		rawFile = new File(DIR+"/ts_"+name+".rts");
		
		/* 
		 * Archive sur 5 minutes / tests de création et à vide
		 */
		Archive archive5 = ts.createArchive(5*60);
		archive5.setWriteStartegy(Archive.WriteStrategy.CHANGE_STEP);
		
		File archive5File = new File(DIR+"/ts_"+name+"_"+(5*60)+".ats");
		Assert.assertTrue(archive5File.exists());
		//on est censé avoir aucun enregistrement dans l'archive, ni même les données de step en cours
		Assert.assertEquals(Archive.HEADER1_LEN,archive5File.length());
		
		/*
		 * Enregistrements
		 */
		Calendar cal = GregorianCalendar.getInstance();
		cal.set(2015, 11, 07, 00, 10);	// 00:10
		
		/*
		 * Premier enregistrement
		 */
		ts.post(cal.getTime(), 1);
		
		//La taille du fichier raw doit correspondre à la taille d'un enregistrement
		int nb = 1;
		Assert.assertEquals(rawFile.length(), RawData.DATA_LEN * nb);
		
		//on est censé n'avoir aucun enregistrement dans l'archive de 5mn
		long expectedLen = Archive.HEADER1_LEN;
		if(archive5.getWriteStartegy() == Archive.WriteStrategy.ALL_POINTS)  expectedLen += archive5.currentStepDataLength(); 
		Assert.assertEquals(expectedLen,archive5File.length());
		
		/*
		 * Deuxième enregistrement : pas de changement de step
		 */
		cal.add(Calendar.MINUTE, 1);	//00:11		T5+1
		nb++;
		ts.post(cal.getTime(), 5);
		Assert.assertEquals(rawFile.length(), RawData.DATA_LEN * nb);
		
		//on est censé n'avoir aucun enregistrement dans l'archive de 5mn
		Assert.assertEquals(expectedLen,archive5File.length());
				
		/*
		 * Troisième enregistrement : changement de step
		 */
		cal.add(Calendar.MINUTE, 5);	//00:16		T5+6
		nb++;
		ts.post(cal.getTime(), 11);
		Assert.assertEquals(rawFile.length(), RawData.DATA_LEN * nb);
		
		//on est censé avoir 1 enregistrement dans l'archive de 5mn, 
		assertNbInArchiveFile(1,archive5,archive5File);	

		//curStep
		AbsCounterArchivePoint point = (AbsCounterArchivePoint)archive5.currentStepPoint();
		Assert.assertEquals((float)11,point.getValue(),0.0001);
		Assert.assertEquals((float)6,point.getDiff(),0.0001);
		Assert.assertFalse(point.overflow);
		
		/*
		 * 4° enregistrement : même step
		 */
		cal.add(Calendar.MINUTE, 1);	//00:17 	T5+7
		nb++;
		ts.post(cal.getTime(), 11);
		
		/*
		 * 5° enregistrement : même step / overflow
		 */
		cal.add(Calendar.MINUTE, 1);	//00:17 	T5+7
		nb++;
		ts.post(cal.getTime(), 1);
		
		/*
		 * 6° enregistrement : changement de step avec overflow sur le step en cours
		 * max=11, value=1, previous=5
		 */
		//curStep
		point = (AbsCounterArchivePoint)archive5.currentStepPoint();
		Assert.assertEquals((float)1,point.getValue(),0.0001);
		Assert.assertEquals((float)7,point.getDiff(),0.0001);
		Assert.assertTrue(point.overflow);
				
		cal.add(Calendar.MINUTE, 9);	//00:26 	T5+16
		nb++;
		ts.post(cal.getTime(), 2);
		Assert.assertEquals(rawFile.length(), RawData.DATA_LEN * nb);
		
		//on est censé avoir 2 enregistrement dans l'archive de 5mn
		assertNbInArchiveFile(2,archive5,archive5File);
		
		
		
		/*
		 * 6° enregistrement
		 */
		cal.add(Calendar.MINUTE, 5);	//00:31		T5+21
		nb++;
		ts.post(cal.getTime(), 16);
		Assert.assertEquals(rawFile.length(), RawData.DATA_LEN * nb);
		
		//on est censé avoir 4 enregistrement dans l'archive de 5mn
		assertNbInArchiveFile(4,archive5,archive5File);
		
		/*
		 * construction d'une archive 15 mn. Elle doit récupérer les valeurs brutes déjà enregistrées
		 */
		Archive archive15 = ts.createArchive(15*60);
		File archive15File = new File(DIR+"/ts_"+name+"_"+(15*60)+".ats");
		Assert.assertTrue(archive15File.exists());
		
		//on est censé avoir 2 enregistrements dans l'archive de 15mn 
		assertNbInArchiveFile(2,archive15, archive15File);
		
		/*
		 * Requete
		 */
		Entry e = ts.getLast();
		Assert.assertEquals(16,e.value,0.0000001);
		Assert.assertEquals(cal.getTimeInMillis()/1000,e.timestamp);
		
		/*
		 * On termine : fermeture du TimeSerieDB
		 */
		if (ts != null)
			ts.close();
	}
	
	
	void assertNbInArchiveFile(int nb, Archive archive, File archiveFile){
		Assert.assertEquals(
				nb * archive.enregLen() + archive.currentStepDataLength() + Archive.HEADER1_LEN,
				archiveFile.length()
				);
	}
}
