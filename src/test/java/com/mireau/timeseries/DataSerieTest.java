package com.mireau.timeseries;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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

import com.mireau.timeseries.ArchiveDataSerie.Type;
import com.mireau.timeseries.RawDataSerie.Entry;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DataSerieTest {
	
	static String TEST_SERIE_NAME = "test-db";
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
		//Création de la TimeSerieDB
		ts = new TimeSeriesDB(TEST_SERIE_NAME,DIR);
		
		rawFile = new File(DIR+"/ts_"+TEST_SERIE_NAME+".rts");
	}

	//Après chaque test
	@After
	public void tearDown() throws Exception {
		/*
		 * On termine : fermeture du TimeSerieDB
		 */
		if (ts != null)
			ts.close();
	}

	
	TimeSeriesDB ts;
	File rawFile;	
	
	@Test
	public void t01_test() throws IOException, ArchiveInitException {
		
		/* 
		 * Archive sur 5 minutes / tests de création et à vide
		 */
		ArchiveDataSerie archive5 = ts.createArchive(5*60, Type.AVERAGE);
		archive5.setWriteStartegy(ArchiveDataSerie.WriteStrategy.CHANGE_STEP);
		Assert.assertEquals(1,ts.archives.size());
		
		File archive5File = new File(DIR+"/ts_"+TEST_SERIE_NAME+"_"+(5*60)+".ats");
		Assert.assertTrue(archive5File.exists());
		//on est censé avoir aucun enregistrement dans l'archive, ni même les données de step en cours
		Assert.assertEquals(ArchiveDataSerie.HEADER1_LEN,archive5File.length());
		
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
		Assert.assertEquals(rawFile.length(), RawDataSerie.DATA_LEN * nb);
		
		//on est censé n'avoir aucun enregistrement dans l'archive de 5mn
		long expectedLen = ArchiveDataSerie.HEADER1_LEN;
		if(archive5.getWriteStartegy() == ArchiveDataSerie.WriteStrategy.ALL_POINTS)  expectedLen += archive5.currentStepDataLength(); 
		Assert.assertEquals(expectedLen,archive5File.length());
		
		/*
		 * Deuxième enregistrement
		 */
		cal.add(Calendar.MINUTE, 1);	//00:11		T5+1
		nb++;
		ts.post(cal.getTime(), 2);
		Assert.assertEquals(rawFile.length(), RawDataSerie.DATA_LEN * nb);
		
		//on est censé n'avoir aucun enregistrement dans l'archive de 5mn
		Assert.assertEquals(expectedLen,archive5File.length());
				
		/*
		 * Troisième enregistrement
		 */
		cal.add(Calendar.MINUTE, 5);	//00:16		T5+6
		nb++;
		ts.post(cal.getTime(), 3);
		Assert.assertEquals(rawFile.length(), RawDataSerie.DATA_LEN * nb);
		
		//on est censé avoir 1 enregistrement dans l'archive de 5mn, 
		assertNbInArchiveFile(1,archive5,archive5File);	
	
		/*
		 * 4° enregistrement
		 */
		cal.add(Calendar.MINUTE, 10);	//00:26 	T5+16
		nb++;
		ts.post(cal.getTime(), 4);
		Assert.assertEquals(rawFile.length(), RawDataSerie.DATA_LEN * nb);
		
		//on est censé avoir 2 enregistrement dans l'archive de 5mn
		assertNbInArchiveFile(2,archive5,archive5File);
		
		/*
		 * 5° enregistrement
		 */
		cal.add(Calendar.MINUTE, 5);	//00:31		T5+21
		nb++;
		ts.post(cal.getTime(), 5);
		Assert.assertEquals(rawFile.length(), RawDataSerie.DATA_LEN * nb);
		
		//on est censé avoir 4 enregistrement dans l'archive de 5mn
		assertNbInArchiveFile(4,archive5,archive5File);
		
		/*
		 * construction d'une archive 15 mn. Elle doit récupérer les valeurs brutes déjà enregistrées
		 */
		ArchiveDataSerie archive15 = ts.createArchive(15*60, Type.AVERAGE);
		ts.buildArchive(15*60, Type.AVERAGE);
		File archive15File = new File(DIR+"/ts_"+TEST_SERIE_NAME+"_"+(15*60)+".ats");
		Assert.assertTrue(archive15File.exists());
		
		//on est censé avoir 2 enregistrements dans l'archive de 15mn 
		assertNbInArchiveFile(2,archive15, archive15File);
		
		/*
		 * Requete
		 */
		Entry e = ts.getLast();
		Assert.assertEquals(5,e.value,0.0000001);
		Assert.assertEquals(cal.getTimeInMillis()/1000,e.timestamp);
	}

	@Test
	public void t02_test() throws IOException, ArchiveInitException {
		Entry e = ts.getLast();
		Assert.assertEquals(5,e.value,0.0000001);
	}
	
	
	void assertNbInArchiveFile(int nb, ArchiveDataSerie archive, File archiveFile){
		Assert.assertEquals(
				nb * AverageArchiveDataSerie.ENREG_LEN + archive.currentStepDataLength() + ArchiveDataSerie.HEADER1_LEN,
				archiveFile.length()
				);
	}
}
