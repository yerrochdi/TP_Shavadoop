import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MasterThread extends Thread implements Runnable
{
	private String m_hostname;
	private String[] m_command;
	private boolean m_hostUsable;
	 
	public MasterThread(String hostname, String[] command) 
	{
	    this.m_hostname = hostname;
	    this.m_command = command;
	    this.m_hostUsable = true;
	}
	public void run()
	{	
		
		if( executeCommand(m_command,this.m_hostname) )
		{
			m_hostUsable = true;
		}
		else
		{
			m_hostUsable = false;
		}
	}
	
	public boolean isHostUsable()
	{
		return m_hostUsable;
	}
	
	public String getHost()
	{
		return m_hostname;
	}
	
	private Boolean executeCommand(String[] command,String host) {

		StringBuffer output = new StringBuffer();
		Date currentTime = new Date();
		SimpleDateFormat h = new SimpleDateFormat ("hh:mm:ss");
		
		
		
		Process p;
		try {
			System.out.println("Lancement Slave => " + host);
			System.out.println("Machine => " + host + " Affichage Avant....."+ h.format(currentTime));
			p = Runtime.getRuntime().exec(command);
			int code = p.waitFor();
			BufferedReader reader =
                            new BufferedReader(new InputStreamReader(p.getInputStream()));
                        String line = "";
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}
			if(code == 0){
				if (output.toString() != "") {
					System.out.println("Machine: "+host+"  retour: "+output.toString());
				}
				return true;
			}
			else {
				return false;
			}
			//Date currentTimeAfter = new Date();
			//System.out.println("Machine => " + host +" Affichage Après....."+ h.format(currentTimeAfter));
			
			
		} catch (Exception e) {
			System.out.println("Machine => " + host +" KO" );
			e.printStackTrace();
			return false;
		}

	}
	
}