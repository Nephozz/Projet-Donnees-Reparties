package config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Project {
    // Chemin du fichier config
    public static String PATH = "/home/nephozz/Documents/Projets-TPs/PDR/hagidoop/config/";

    // Définition des ports constant
    public static final int NRW_PORT = 8000;
    public static final int HDFS_PORT = 5002;

    // HashMap des host avec leurs ports
    public static HashMap<Integer, String> servers = new HashMap<Integer, String>();

    public Project() {}

    /*
	 * readConfigFile : lit le fichier de configuration et remplit les listes machines et ports
	 */
	private HashMap<Integer, String> readConfigFile() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(PATH + "config.txt"));
        String line;

        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#")) { continue; }

            String host = line.split(" ")[0];
            int port = Integer.parseInt(line.split(" ")[1]);
            
            servers.put(port, host);
        }
        reader.close();
        return servers;
    }

    public void setPath(String path) {
        PATH = path;
    }

    public static void main(String[] args) {
        Project p = new Project();
        try {
            p.readConfigFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
