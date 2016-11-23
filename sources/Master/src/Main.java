import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.FileUtils;


public class Main {
	
//On passe le chemin du fichier .properties au master jar
public static void main(String[] args) throws IOException, InterruptedException {
	
	String hostsFilename;
	String CheminReps; 
	String CheminJar; 
	String userName; 
	String inputFilename;
	
	if (args[0] != null) 
	{
		Properties prop = new Properties();
		InputStream input = null;
		
		input = new FileInputStream(args[0]);
		prop.load(input);	
		hostsFilename = prop.getProperty("hostsFilename");
		CheminReps = prop.getProperty("CheminReps");
		CheminJar = prop.getProperty("CheminJar");
		userName = prop.getProperty("userName");
		inputFilename = prop.getProperty("inputFilename");
		
	} else
	{
		hostsFilename = "/cal/homes/errochdi/liste_machines.txt";
		CheminReps = "/cal/homes/errochdi/MRFILES";
		CheminJar = "/cal/homes/errochdi/Shavadoop-master2/Shavadoop/Slave";
		userName = "errochdi";
		inputFilename = "/cal/homes/errochdi/Input.txt";
	}
	

	


	
	
	/* Nettoyage du rep de travail .... */
	File RepANettoyer = new File(CheminReps);  
	FileUtils.cleanDirectory(RepANettoyer);
	
	
    /*Création structure du projet
     */
	File SXdir = new File(CheminReps+"/SX");
	File UMdir = new File(CheminReps+"/UM");
	File RMdir = new File(CheminReps+"/RM");
	File SMdir = new File(CheminReps+"/SM");
	File MAPdir = new File(CheminReps+"/MAP");
	
	SXdir.mkdir();
	UMdir.mkdir();
	RMdir.mkdir();
	SMdir.mkdir();
	MAPdir.mkdir();
    
	
	
	System.out.println("--------------------------------------------------");
	System.out.println("\tDébut du wordCount en Shavadoop");
	System.out.println("--------------------------------------------------");
	int startTime = (int) System.currentTimeMillis();
	
	/* Exécution du wordcount par Shavadoop */

	//Instanciation Master
	Master masterNode = new Master(hostsFilename,inputFilename,CheminReps,CheminJar,userName);	
	int InitMasterTime = (int) System.currentTimeMillis();

	//Clean file:On supprime tous les mots comtenus dans le fichier "motsIgnores.txt"
	masterNode.clean_file(inputFilename);
	System.out.println("--------------------------------------");
	System.out.println("Instanciation et nettoyage- " + (InitMasterTime - startTime) + " ms.");
	System.out.println("--------------------------------------");
	
	
	
	
	//Split
	masterNode.splitting();
	int SplitTime = (int) System.currentTimeMillis();
	System.out.println("--------------------------------------");
	System.out.println("Spliting - " + (SplitTime - InitMasterTime) + " ms.");
	System.out.println("--------------------------------------");
	
	//Mapping
	masterNode.Mapping();
	int MappingTime = (int) System.currentTimeMillis();
	System.out.println("--------------------------------------");
	System.out.println("Mapping - " + (MappingTime - SplitTime) + " ms.");
	System.out.println("--------------------------------------");
	
	//Construction des dico cle UMX
	masterNode.keysEditor();
	int KeyEditorTime = (int) System.currentTimeMillis();
	System.out.println("--------------------------------------");
	System.out.println("Keys building - " + (KeyEditorTime - MappingTime) + " ms.");
	System.out.println("--------------------------------------");
	
	//Shuffling
	masterNode.Shuffling();
	int ShufflingTime = (int) System.currentTimeMillis();
	System.out.println("--------------------------------------");
	System.out.println("Shuffling  - " + (ShufflingTime - KeyEditorTime) + " ms.");
	System.out.println("--------------------------------------");
	
	
	//Reducing
	masterNode.reducing();
	int ReducingTime = (int) System.currentTimeMillis();
	System.out.println("--------------------------------------");
	System.out.println("Reducing - " + (ReducingTime - ShufflingTime) + " ms.");
	System.out.println("--------------------------------------");
	
	
	System.out.println("--------------------------------------");
	System.out.println("Duée totale - " + (ReducingTime - startTime) + " ms.");
	System.out.println("--------------------------------------");
	
	
}


}
