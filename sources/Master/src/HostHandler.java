import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


public class HostHandler {
	
	private String m_host;

	public HostHandler(String m_hostname) {
		// TODO Auto-generated constructor stub
		m_host = m_hostname;
	}


	//Exécution d'une commande shell
	public boolean exec(String[] m_command) throws IOException, InterruptedException {

		// 
		ProcessBuilder pb = new ProcessBuilder(m_command);
		Process process = pb.start();
		
		int errCode = process.waitFor();
		
		//AfficheurFlux fluxSortie = new AfficheurFlux(procesexecs.getInputStream());
		
		if (errCode == 0) {
			System.out.println("Machine "+m_host+" ==> " + output(process.getInputStream()));
			return true;
		}
		else {
			System.out.println("Connexion machine "+m_host+" KO !!!!");
			return false;
		}
		
	}
	private static String output(InputStream inputStream) throws IOException {
		StringBuilder sb = new StringBuilder();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(inputStream));
			String line = null;
			while ((line = br.readLine()) != null) {
				sb.append(line + System.getProperty("line.separator"));
			}
		} finally {
			br.close();
		}
		return sb.toString();
	}

}
