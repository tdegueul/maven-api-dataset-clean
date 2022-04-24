package mcr;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;

import com.opencsv.CSVReaderHeaderAware;

import nl.cwi.swat.aethereal.AetherDownloader;

public class OutliersDataset {

	private final AetherDownloader downloader;
	
	private static final Logger logger = LogManager.getLogger(OutliersDataset.class);
	private static final RScriptRunner runner = new RScriptRunner(logger);
	
	static final String RQ0_OUTLIERS = "data/rq0/rq0-outliers-sample.csv";
	static final String RQ1_OUTLIERS = "data/rq1/rq1-outliers-sample.csv";
	
	static final String RQ0_OUTLIERS_SAMPLING_SCRIPT = "scripts/rq0-outliers-sampling.R";
	static final String RQ1_OUTLIERS_SAMPLING_SCRIPT = "scripts/rq1-outliers-sampling.R";
	
	public OutliersDataset(int qps) {
		this.downloader = new AetherDownloader(qps);
	}
	
	public void sampleOutliers() {
		try {
			// TODO: change input
			runner.runSampling(RQ0_OUTLIERS_SAMPLING_SCRIPT, RQ0_OUTLIERS, RQ0_OUTLIERS);
		} 
		catch (InterruptedException e) {
			logger.error("Interrupted", e);
		} 
		catch (IOException e) {
			logger.error("Interrupted", e);
		}
	}
	
	public void sampleDeltaOutliers() {
		try {
			// TODO: change input
			runner.runSampling(RQ1_OUTLIERS_SAMPLING_SCRIPT, RQ1_OUTLIERS, RQ1_OUTLIERS);
		} 
		catch (InterruptedException e) {
			logger.error("Interrupted", e);
		} 
		catch (IOException e) {
			logger.error("Interrupted", e);
		}
	}
	
	public void fetchOutliers() {
		try (CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(RQ0_OUTLIERS))) {
			Map<String, String> line;
			
			while ((line = reader.readMap()) != null) {
				String group = line.get("group");
				String artifact = line.get("artifact");
				String v1 = line.get("v1");
				
				Artifact apiOld = downloader.downloadArtifact(new DefaultArtifact(group, artifact, "sources", "jar", v1));
			}
		}
		catch (FileNotFoundException e) {
			logger.error("IOE:", e);
		} 
		catch (IOException e) {
			logger.error("IOE:", e);
		}
	}
	
	public void fetchDeltaOutliers() {
		try (CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(RQ1_OUTLIERS))) {
			Map<String, String> line;
			
			while ((line = reader.readMap()) != null) {
				String group = line.get("group");
				String artifact = line.get("artifact");
				String v1 = line.get("v1");
				String v2 = line.get("v2");
				String srcDelta = line.get("delta");
			
				Artifact apiOld = downloader.downloadArtifact(new DefaultArtifact(group, artifact, "sources", "jar", v1));
				Artifact apiNew = downloader.downloadArtifact(new DefaultArtifact(group, artifact, "sources", "jar", v2));
				
				String dir = FilenameUtils.getFullPath(apiOld.getFile().getAbsolutePath());
				String targDelta = FilenameUtils.concat(dir, FilenameUtils.getName(srcDelta));
				
				if (new File(srcDelta).exists()) {
					FileUtils.copyFile(new File(srcDelta), new File(targDelta));
				}
				else {
					logger.error("IOE: " + srcDelta + " does not exist.");
				}
			}
		} 
		catch (FileNotFoundException e) {
			logger.error("IOE:", e);
		} 
		catch (IOException e) {
			logger.error("IOE:", e);
		}
	}
	
	public static void main(String[] args) {
		OptionGroup group = new OptionGroup()
			.addOption(Option.builder("sampleAll")
				.desc("Build the rq0-outliers.csv dataset").build())
			.addOption(Option.builder("sample")
				.desc("Build the rq0-outliers.csv dataset").build())
			.addOption(Option.builder("sampleDelta")
				.desc("Build the rq1-outliers.csv dataset").build())
			.addOption(Option.builder("fetchAll")
				.desc("Fetch outliers").build())
			.addOption(Option.builder("fetch")
				.desc("Fetch outliers").build())
			.addOption(Option.builder("fetchDelta")
				.desc("Fetch delta outliers").build());
		group.setRequired(true);
		
		HelpFormatter formatter = new HelpFormatter();
		Options opts = new Options().addOptionGroup(group);
		
		try (FileInputStream fis = new FileInputStream("config.properties")) {	
			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(opts, args);
			
			Properties props = new Properties();
			props.load(fis);
			int qps = Integer.parseInt(props.getProperty("aether_qps"));
			OutliersDataset prog = new OutliersDataset(qps);
			boolean sample = cmd.hasOption("sampleAll");
			boolean fetch = cmd.hasOption("fetchAll");
			
			if (sample || cmd.hasOption("sample")) {
				Instant t1 = Instant.now();
				prog.sampleOutliers();
				Instant t2 = Instant.now();
				logger.info("", Duration.between(t1, t2).toMinutes());
			}
			
			if (sample || cmd.hasOption("sampleDelta")) {
				Instant t1 = Instant.now();
				prog.sampleDeltaOutliers();
				Instant t2 = Instant.now();
				logger.info("", Duration.between(t1, t2).toMinutes());	
			}
			
			if (fetch || cmd.hasOption("fetch")) {
				Instant t1 = Instant.now();
				prog.fetchOutliers();
				Instant t2 = Instant.now();
				logger.info("", Duration.between(t1, t2).toMinutes());
			}
			
			if (fetch || cmd.hasOption("fetchDelta")) {
				Instant t1 = Instant.now();
				prog.fetchDeltaOutliers();
				Instant t2 = Instant.now();
				logger.info("", Duration.between(t1, t2).toMinutes());
			}
		} 
		catch (FileNotFoundException e) {
			logger.error("E:", e);
		} 
		catch (IOException e) {
			logger.error("E:", e);
		} 
		catch (ParseException e) {
			formatter.printHelp("outliersDataset", opts);
		}
	}
}
