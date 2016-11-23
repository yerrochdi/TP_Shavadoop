import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class Slave {

	public static void main(String[] args) throws InterruptedException,
			IOException {

		/************** Création de fichiers UMX à partir des SX *********************/
		if (args[0].equals("modeSXUMX")) {
			// On récupere le nom di fichier
			String sxName = args[1];
			String sxIndex = sxName.split("S")[1];
			String filePath = args[2];
			String umName = filePath+"/UM/UM" + sxIndex;

			String sxAddress = filePath+"/SX/" + sxName;
			FileReader in = new FileReader(sxAddress);
			BufferedReader bin = new BufferedReader(in);
			String line = bin.readLine();
			String[] words = line.split("\\s+");
			bin.close();

			ArrayList<String> ListeMots = new ArrayList<String>();
			for (String word : words)
				ListeMots.add(word);

			PrintWriter out = new PrintWriter(umName);
			for (String word : ListeMots)
				out.append(word + " 1\n");
			out.close();
		}
		/************** Création de fichier SM à partir des fichier UMX *********************/
		else if (args[0].equals("modeUMXSMX")) {

			String key = args[1];
			String smxName = args[2];
			String filePath = args[3];
			String smxAddress = filePath+"/SM/" + smxName;
			PrintWriter SMXFile = new PrintWriter(smxAddress);
			for(int i = 4 ; i < args.length ; i++) {
				String umxName = filePath+"/UM/" + args[i];
				FileReader UmxFile = new FileReader(umxName);
				BufferedReader UmxContenu = new BufferedReader(UmxFile);
				

				
				String currentLine = UmxContenu.readLine();
				
				while(currentLine != null) {
					
					String currentKey = currentLine.split("\\s+")[0];
					
					if(currentKey.equals(key))
						SMXFile.append(currentLine + "\n");	
					
					currentLine = UmxContenu.readLine();
				}
				UmxContenu.close();	
			}
			SMXFile.close();
			
			/************** Création de fichier RM à partir des fichier SMX *********************/
			FileReader SmxOut = new FileReader(smxAddress);
			BufferedReader FileSmx = new BufferedReader(SmxOut);
			//On récupère le numéro du fichier
			String numSMX = smxName.split("M+")[1];
			String rmxName = "RM" + numSMX;
			String rmxAddress = filePath+"/RM/" + rmxName;
			PrintWriter RmxFile = new PrintWriter(rmxAddress);
			int comptWord = 0;
			while(FileSmx.readLine() != null)
				comptWord ++;
			RmxFile.write(key + " " + comptWord);
			
			System.out.println("Mot: "+ key + " est présent " + comptWord + " fois");
			FileSmx.close();
			RmxFile.close();
			
			
		}

	}

}
