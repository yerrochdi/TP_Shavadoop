import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;


public class Master {

	
	//Déclaration des variables master
	private String ListeMachinesPotentiels;
	private ArrayList<String> m_listOfUsableHosts;
	private String m_inputFilename;
	//private int m_nbSlaveNodes;
	private Set<String> keys = new HashSet<String>();
	private HashMap<String,HashSet<String>> KeyUmx = new HashMap<String, HashSet<String>>();
	private HashMap<String,HashSet<String>> UmxMachine = new HashMap<String, HashSet<String>>();
	private HashMap<String,HashSet<String>> RmxMachine = new HashMap<String, HashSet<String>>();
	private int NbFichiers = 0;
	private String CheminReps;
	private String CheminSlave;
	private String userName;
	//private long timeout = 3000L;
	
	//Constructeur du Master
	public Master(String hostsFilename,String inputFilename,String PathReps,String CheminJar,String user) throws IOException,
			InterruptedException {
		// TODO Auto-generated constructor stub
		ListeMachinesPotentiels = hostsFilename;
		CheminReps = PathReps;
		CheminSlave = CheminJar;
		userName = user;
		m_inputFilename = CheminJar + "/FileClean.txt";
		//m_inputFilename = inputFilename;
		m_listOfUsableHosts = new ArrayList<String>();
		getMachinesConnectees();
		// On lance les threads sur les machines dispos
		System.out.println("lancement jar slaves....");

	}

	
	//Mapping: création des fichiers UMx
	public void Mapping() throws InterruptedException {
		System.out.println("Début Mapping....");
		// TODO Auto-generated method stub
		ArrayList<MasterThread> listOfThreads = new ArrayList<MasterThread>();
		
		
		//To do: faire une boucle sur le nombre de lignes, si le nombre de ligne est inférieur au nombre de machines on prend la machine correspondante à la ligne traitée sinon on prend le modulo
		for (int i = 0; i < NbFichiers; i++) {			
			int indice_machine = (i %  (m_listOfUsableHosts.size()));
			String host = m_listOfUsableHosts.get(indice_machine);
			HashSet<String> umInd = new HashSet<String>();
			umInd.add("UM"+i);
			UmxMachine.put(host,umInd);	
			String[] cmd = {
			"/bin/bash",
			"-c",
			"ssh "+userName+"@"
					+ host
					+ " java -jar "+CheminSlave+"/Slave.jar modeSXUMX S"+i+".txt "+CheminReps};
			
			
			MasterThread tmpThread = new MasterThread(host, cmd);
			listOfThreads.add(tmpThread);
		}
		
		
		
		
		// Une fois la liste des machines chargée on test si elles sont
		// pingables (connectées)
		for (MasterThread th : listOfThreads) {
			th.start();
		}
		for (MasterThread th : listOfThreads) {
			th.join();
		}
		System.out.println(".......  Fin Mapping ...........");
	}

	
	//Shuffling: Création des des fichiers RMx 
	public void Shuffling() throws InterruptedException {
		
		System.out.println(".......Debut Shuffling...........");
		
		ArrayList<MasterThread> listOfThreads = new ArrayList<MasterThread>();
		String argument = null;
		int i = 0;
		
		//Créer les threads pour chaque clef
		for(String key : keys) {
			//argument = "modeUMXSMX Car SM1 UM1 UM2"
			argument = "modeUMXSMX " + key + " SM"+i+".txt "+CheminReps;
			
			int indice_machine = (i %  (m_listOfUsableHosts.size()));
			String host = m_listOfUsableHosts.get(indice_machine);
			
			//On boucle sur les UMX associées à la clef
			HashSet<String> cleUmx = KeyUmx.get(key);
			for (String umx: cleUmx) {
				argument = argument + " " + umx;
			}




			HashSet<String> RmInd = new HashSet<String>();
			RmInd.add("RM"+i);
			RmxMachine.put(host,RmInd);	
			
			String[] cmd = {
			"/bin/bash",
			"-c",
			"ssh "+userName+"@"
					+ host
					+ " java -jar "+CheminSlave+"/Slave.jar "+argument};
			
			MasterThread tmpThread = new MasterThread(host, cmd);
			listOfThreads.add(tmpThread);
			i++;
		}

		// Une fois la liste des machines chargée on test si elles sont
		// pingables (connectées)
		for (MasterThread th : listOfThreads) {
			th.start();
		}
		for (MasterThread th : listOfThreads) {
			th.join();
		}
		System.out.println(Arrays.deepToString(RmxMachine.entrySet().toArray()));
		System.out.println(".......Fin Shuffling...........");
	}
	
	
	//Lecture du fichier des machines 
	private List<String> getPotentialHosts() throws IOException {
		
		System.out.println(".......Début test Machines...........");
		Scanner FichierMachines = new Scanner(new FileReader(
				ListeMachinesPotentiels));
		String MachineName = null;
		ArrayList<String> hostList = new ArrayList<String>();
		while (FichierMachines.hasNextLine()) {
			MachineName = FichierMachines.nextLine();
			hostList.add(MachineName);
		}
		FichierMachines.close();
		System.out.println(".......Fin test Machines...........");
		return hostList;
	}
    
	
	
	//Recuperer les machines connectées
	public void getMachinesConnectees() throws IOException,
			InterruptedException {
		List<String> potentialHosts = getPotentialHosts();
		ArrayList<MasterThread> listOfThreads = new ArrayList<MasterThread>();
		// On boucle sur la liste des machines
		for (String host : potentialHosts) {
			String[] cmd = { "/bin/bash", "-c",
					"ssh "+userName+"@" + host + " echo OK" };
			MasterThread tmpThread = new MasterThread(host, cmd);
			listOfThreads.add(tmpThread);
		}
		//m_nbSlaveNodes = listOfThreads.size();
		// Une fois la liste des machines chargée oSystemn test si elles sont
		// pingables (connectées)
		for (MasterThread th : listOfThreads)
			th.start();

		for (MasterThread th : listOfThreads)
			th.join();
		
		FileWriter fw = new FileWriter("Liste_machine_connectees.txt");
		System.out.println("Ecriture fichiergetMachinesConnectees des machines connectées....");
		for (MasterThread th : listOfThreads) {
			if (th.isHostUsable()) {
				m_listOfUsableHosts.add(th.getHost());
				// Ecriture du fichier contenant les machines actives
				// (pingables)
				fw.write(th.getHost() + "\n");
			}
		}	
		
		System.out.println("--------------------------------------------------");
		System.out.println("\tFin Etape 0");
		System.out.println("--------------------------------------------------");
		fw.close();
	}
	
	
	public void splitting() throws IOException {
		try {
			System.out.println("Début : Splitting");
	    	long startTime = System.currentTimeMillis();
	    	
	    	FileReader in = new FileReader(m_inputFilename);
	    	BufferedReader bin = new BufferedReader(in);
	    	
	    	String line = bin.readLine();
	    	
	    	do {
	    		if (line.trim().length() != 0) {
	    			String fileName = CheminReps+"/SX/S" + NbFichiers + ".txt";
	        		PrintWriter out = new PrintWriter(fileName);
	        		out.println(line);    		
	        		out.close();
	        		NbFichiers++;
				}
	    		line = bin.readLine();
	    	} while (line != null);
	    	
	    	bin.close();
	    	long endTime   = System.currentTimeMillis();
	    	long totalTime = endTime - startTime;
	    	System.out.println("Fin : Splitting - " + totalTime + " ms.");
			
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}

	}
	
	
	//Reducing: Fusion des fichiers RMx pour faire un seul fichier en sortie
	public void reducing() throws IOException {
		System.out.println("Début : Reducing");
    	long startTime = System.currentTimeMillis();
    	HashMap<String, Integer> mapReduce = new HashMap<String, Integer>();
    	
    	PrintWriter fichierFinal = new PrintWriter(CheminReps+"/MAP/map_reduce.txt");
    	
    	//recevoir les RMx
    	for(int i = 0 ; i < keys.size() ; i++) {
    		try {
        		FileReader RmFile = new FileReader(CheminReps+"/RM/RM" + i + ".txt");
        		BufferedReader RmContenu = new BufferedReader(RmFile);
        		
        		String[] lineRM = RmContenu.readLine().split("\\s+");
        		String key = lineRM[0];
        		int value = Integer.parseInt(lineRM[1]);
        		mapReduce.put(key, value);
        		RmContenu.close();
        		
			} catch (Exception e) {
				// TODO: handle exception
			}
    		

    	}
    	
    	//Trier le fichier de sortie
    	
    	mapReduce = (HashMap<String, Integer>) sortByValue(mapReduce);
    	
    	long endTime   = System.currentTimeMillis();
    	long totalTime = endTime - startTime;
    	System.out.println("Fin : Reducing - " + totalTime + " ms.");
    	
    	//Les fusionner
    	for(String key : mapReduce.keySet())
    		fichierFinal.append(key + " " + mapReduce.get(key) + "\n");
    	
    	fichierFinal.close();
    	
    	System.out.println("Fin : Reducing");
	}
	
	//Création des dicos
	public Set<String> keysEditor() throws IOException {
		
		System.out.println(".......Debut Creation Cle UMX...........");
		//HashSet<String> ums = new HashSet<String>();
    	//trouve toutes les clefs dans les UMx avec le x associé
    	for(int k = 0 ; k < NbFichiers ; k++) {
    		try {
    			Scanner fichierUm = new Scanner(new FileReader(
    					CheminReps+"/UM/UM" + k + ".txt"));
        		 while(fichierUm.hasNextLine())
        		 {
        			String currentLine = fichierUm.nextLine();
        			if(currentLine.split("\\s+")[0] != null) {
        				String currentKey = currentLine.split("\\s+")[0];
        				keys.add(currentKey);
        			}
        		}
        		 fichierUm.close();
			} catch (Exception e) {
				// TODO: handle exception
				System.out.println(e);
			}
    		
    	}
   
    	keys.remove(null); //Elnlève la ligne vide
    	
    	//associer à chaque clef les UMx qui les contient
    	PrintWriter out3 = new PrintWriter(CheminReps+"/UM/Clés_UMx");
    	for(String key : keys) {
    		HashSet<String> umIndexes = new HashSet<String>();
    		for(int k = 0 ; k < NbFichiers ; k++) {
    			try {
    				Scanner in3_2 = new Scanner(new FileReader(
    						CheminReps+"/UM/UM" + k +".txt"));
        			//BufferedReader bin3_2 = new BufferedReader(in3_2);
        			while(in3_2.hasNextLine())
        			{
        				String currentLine = in3_2.nextLine();
        				String currentKey = currentLine.split("\\s+")[0];
        				if(currentKey.equals(key) && !(umIndexes.contains(k)))
        				{
        					umIndexes.add("UM"+k+".txt");
        				}
        			}
        			in3_2.close();
					
				} catch (Exception e) {
					// TODO: handle exception
					System.out.println(e);
				}
    			
    		}
    		KeyUmx.put(key, umIndexes);
    	}
        out3.close();
    	System.out.println(Arrays.deepToString(KeyUmx.entrySet().toArray()));
    	System.out.println(Arrays.deepToString(UmxMachine.entrySet().toArray()));
    	
    	System.out.println(".......Fin Creation Cle UMX...........");
    	return keys;
	}

	
	public void clean_file(String inputFilename) throws IOException {
		
		File file = new File(inputFilename);
		File wordstodelete = new File(CheminSlave+"/motsIgnores.txt");
		String charset = "UTF-8";
		try {
			BufferedReader readertodelete = new BufferedReader(new InputStreamReader(new FileInputStream(wordstodelete), charset));
			
			
	        BufferedReader reader = new BufferedReader(new FileReader(file));
	        String line = "", oldtext = "";
	        while((line = reader.readLine()) != null)
	            {
	        		String lineTraitee = line;
		        	if (lineTraitee.trim().length() != 0) {
		        		oldtext += line + "\n";
		        	}
	            }
	        reader.close();
	        
	        //To replace a line in a file
	        for (String delete; (delete = readertodelete.readLine()) != null;) {
	        	System.out.println("Mot à supprimer : " + delete);
	        	oldtext = oldtext.toLowerCase().replaceAll(" "+delete.toLowerCase()+" ", " ");
	        }
	        
	        System.out.println("Mot à supprimer : ");
	        String newtext = oldtext;
	        
	        FileWriter writer = new FileWriter(m_inputFilename);
	        writer.write(newtext);
	        writer.close();
	        readertodelete.close();
			
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}
		
		
	}
	
	//Fonction de tri
	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> unsortMap) {

	    List<Map.Entry<K, V>> list =
	            new LinkedList<Map.Entry<K, V>>(unsortMap.entrySet());

	    Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
	        public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
	            return -1 * (o1.getValue()).compareTo(o2.getValue());
	        }
	    });

	    Map<K, V> result = new LinkedHashMap<K, V>();
	    for (Map.Entry<K, V> entry : list) {
	        result.put(entry.getKey(), entry.getValue());
	    }

	    return result;

	}
	
}
