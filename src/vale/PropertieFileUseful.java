package vale;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

public class PropertieFileUseful {

	public static Properties prop = null;

	private static Logger logger = Logger.getLogger(PropertieFileUseful.class.getName());

	public PropertieFileUseful(String fileName) throws IOException {
		prop = new Properties();

		InputStream input = null;

		// try {

		input = new FileInputStream(fileName);

		// load a properties file
		prop.load(input);

		// System.out.println(prop.getProperty("Teste"));

		// } catch (IOException ex) {
		// ex.printStackTrace();
		// } finally {
		// if (input != null) {
		// try {
		// input.close();
		// } catch (IOException e) {
		// // e.printStackTrace();
		// logger.log(Level.SEVERE, "Load Propertie File", e);
		// }
		// }
		// }

	}

	public static String getProp(String prp) {
		return prop.getProperty(prp);

	}

}
