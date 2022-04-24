package mcr;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

public class RScriptRunner {
	
	private final Logger logger;
	private String rscript;
	
	public RScriptRunner(Logger logger) {
		this.logger = logger;
		
		try (FileInputStream is = new FileInputStream("config.properties")) {
			Properties props = new Properties();
			props.load(is);
			this.rscript = props.getProperty("rscript");
		}
		catch (IOException e) {
			logger.error("IO:", e);
			this.rscript = "";
		}
	}
	
	public void runSampling(String scriptPath, String inputPath, String outputPath) throws InterruptedException, IOException {
		ProcessBuilder pb = new ProcessBuilder(rscript, scriptPath, inputPath, outputPath);
		pb.inheritIO();
		
		if (pb.start().waitFor() != 0) {
			logger.error("Failed running the R sampling script");
		} 
		else {
			BufferedReader br = new BufferedReader(new FileReader(outputPath));
			logger.info("Sampling done: {} resulting libraries", br.lines().count());
			br.close();
		}
	}

	public int sampleSize(int n, float e, float p) {
		try {
			ProcessBuilder pb = new ProcessBuilder(rscript, "scripts/sample-size.R", "" + n, "" + e, "" + p);
			pb.inheritIO();

			if (pb.start().waitFor() != 0) {
				logger.error("Failed running the R sampling script");
			} 
			else {
				try (BufferedReader br = new BufferedReader(new FileReader("sample-size.tmp"))) {
					return Integer.parseInt(br.lines().findFirst().get());
				}
			}
		} catch (InterruptedException | IOException ex) {
			logger.error(ex);
		}

		return -1;
	}
}
