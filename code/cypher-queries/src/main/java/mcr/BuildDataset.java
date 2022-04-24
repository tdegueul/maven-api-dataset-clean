package mcr;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.rascalmpl.interpreter.Evaluator;
import org.rascalmpl.interpreter.NullRascalMonitor;
import org.rascalmpl.interpreter.env.GlobalEnvironment;
import org.rascalmpl.interpreter.env.ModuleEnvironment;
import org.rascalmpl.interpreter.load.StandardLibraryContributor;
import org.rascalmpl.library.Prelude;
import org.rascalmpl.uri.URIUtil;
import org.rascalmpl.util.ConcurrentSoftReferenceObjectPool;
import org.rascalmpl.values.ValueFactoryFactory;

import com.google.common.collect.ImmutableMap;
import com.opencsv.CSVReaderHeaderAware;

import io.usethesource.vallang.IBool;
import io.usethesource.vallang.IInteger;
import io.usethesource.vallang.IMap;
import io.usethesource.vallang.ISet;
import io.usethesource.vallang.ISourceLocation;
import io.usethesource.vallang.IString;
import io.usethesource.vallang.IValue;
import io.usethesource.vallang.IValueFactory;
import mcr.SemVerUtils.SemInfo;
import mcr.SemVerUtils.SemLevel;
import mcr.data.Upgrade;
import nl.cwi.swat.aethereal.AetherDownloader;

public class BuildDataset {
	private final String host;
	private final String user;
	private final String password;
	private final int maxThreads;
	private final AetherDownloader downloader;

	private Map<Path, IValue> m3s = new ConcurrentHashMap<>();

	public static final String LIBRARIES_RAEMAEKERS      = "data/libraries-raemaekers.csv";
	public static final String VERSIONS                  = "data/gen/versions.csv";
	public static final String VERSIONS_RAEMAEKERS       = "data/gen/versions-raemaekers.csv";
	public static final String DELTAS                    = "data/gen/deltas.csv";
	public static final String DELTAS_RAEMAEKERS         = "data/gen/deltas-raemaekers.csv";
	public static final String DELTAS_SAMPLE             = "data/gen/deltas-sample.csv";
	public static final String DELTAS_RAEMAEKERS_SAMPLE  = "data/gen/deltas-sample-raemaekers.csv";
	public static final String CLIENTS                   = "data/gen/clients.csv";
	public static final String CLIENTS_RAEMAEKERS        = "data/gen/clients-raemaekers.csv";
	public static final String DETECTIONS                = "data/gen/detections.csv";
	public static final String DETECTIONS_RAEMAEKERS     = "data/gen/detections-raemaekers.csv";
	public static final String DELTAS_CLEAN              = "data/gen/deltas-clean.csv";
	public static final String DELTAS_RAEMAEKERS_CLEAN   = "data/gen/deltas-raemaekers-clean.csv";
	public static final String UPGRADES                  = "data/gen/upgrades.csv";
	public static final String UPGRADES_RAEMAEKERS       = "data/gen/upgrades-raemaekers.csv";
	public static final String UPGRADES_MAJOR            = "data/gen/upgrades-major.csv";
	public static final String UPGRADES_MAJOR_RAEMAEKERS = "data/gen/upgrades-major-raemaekers.csv";
	public static final String UPGRADES_MINOR            = "data/gen/upgrades-minor.csv";
	public static final String UPGRADES_MINOR_RAEMAEKERS = "data/gen/upgrades-minor-raemaekers.csv";
	public static final String UPGRADES_PATCH            = "data/gen/upgrades-patch.csv";
	public static final String UPGRADES_PATCH_RAEMAEKERS = "data/gen/upgrades-patch-raemaekers.csv";
	public static final String UPGRADES_DEV              = "data/gen/upgrades-dev.csv";
	public static final String UPGRADES_DEV_RAEMAEKERS   = "data/gen/upgrades-dev-raemaekers.csv";
	public static final String UPGRADES_DETECTIONS       = "data/gen/upgrades-detections.csv";

	public static final String DELTAS_PATH     = "data/gen/deltas/";
	public static final String CLIENTS_PATH    = "data/gen/clients/";
	public static final String DETECTIONS_PATH = "data/gen/detections/";
	public static final String M3S_PATH        = "data/gen/m3s/";

	public static final String CLIENTS_SAMPLE_SCRIPT   = "scripts/sampling-clients.R";
	public static final String DELTAS_SAMPLE_SCRIPT    = "scripts/sampling-libraries.R";
	public static final String UPGRADES_SAMPLE_SCRIPT  = "scripts/sampling-upgrades.R";

	private static final Logger logger = LogManager.getLogger(BuildDataset.class);

	public BuildDataset(String host, String user, String pwd, int maxThreads, int qps) {
		this.host       = host;
		this.user       = user;
		this.password   = pwd;
		this.downloader = new AetherDownloader(qps);

		int sysThreads = Runtime.getRuntime().availableProcessors();
		if (maxThreads > sysThreads || maxThreads <= 0)
			this.maxThreads = sysThreads;
		else
			this.maxThreads = maxThreads;
	}

	public void cleanDB() {
		try (
			Driver driver = GraphDatabase.driver(host, AuthTokens.basic(user, password));
			Session session = driver.session(AccessMode.WRITE)
		) {
			logger.info("Cleaning DB...");

			// Remove all non-(test|compile) dependencies
			StatementResult result = session.run(
				"MATCH ()-[d:DEPENDS_ON]->() " +
				"WHERE NOT d.scope IN [\"Compile\", \"Test\"] " +
				"DELETE d"
			);

			logger.info(result.summary());
		}
	}

	public void buildUpgradesDataset() {
		buildUpgradesDataset(DELTAS_CLEAN, UPGRADES);
	}

	public void buildUpgradesRaemaekersDataset() {
		buildUpgradesDataset(DELTAS_RAEMAEKERS_CLEAN, UPGRADES_RAEMAEKERS);
	}

	public void buildUpgradesDataset(String deltas, String upgrades) {
		File upgradesFile = new File(upgrades);

		try (
			CSVReaderHeaderAware deltasReader = new CSVReaderHeaderAware(new FileReader(deltas));
			FileWriter upgradesCsv = new FileWriter(upgrades, true);
		) {
			// Skip if existing
			if (upgradesFile.length() > 0) {
				logger.info("{} exists. Skipping.", upgrades);
				return;
			}

			upgradesCsv.append("lgroup,lartifact,lv1,lv2,level,year,jar_v1,jar_v2,cgroup,cartifact,cversion,cyear,delta\n");

			Map<String, String> line;
			Map<String, Map<String, String>> brokenReleases = new HashMap<>();
			while ((line = deltasReader.readMap()) != null) {
				// We're only interested in broken upgrades here
				if (Integer.parseInt(line.get("bcs_clean_stable")) > 0) {
					brokenReleases.put(
						String.format("%s:%s:%s", line.get("group"), line.get("artifact"), line.get("v1")),
						line
					);
				}
			}

			try (
				Driver driver = GraphDatabase.driver(host, AuthTokens.basic(user, password));
				Session session = driver.session(AccessMode.READ);
			) {
				logger.info("Retrieving clients of broken upgrades to build {}...", upgrades);

				StatementResult result = session.run(
					"MATCH  (c)-[:DEPENDS_ON]->(l) " +
					"WHERE  l.coordinates IN {s} " +
					"RETURN DISTINCT " +
						"l.groupID AS lgroup, " +
						"l.artifact AS lartifact, " +
						"l.version AS lversion, " +
						"c.groupID AS cgroup, " +
						"c.artifact AS cartifact, " +
						"c.version AS cversion, " +
						"datetime(c.release_date).year AS cyear",
					ImmutableMap.<String, Object>builder()
						.put("s", brokenReleases.keySet())
						.build()
				);

				Map<String, Upgrade> latestClients = new HashMap<>();
				while (result.hasNext()) {
					Record row = result.next();

					String lgroup    = row.get("lgroup").asString();
					String lartifact = row.get("lartifact").asString();
					String lv1       = row.get("lversion").asString();
					String cgroup    = row.get("cgroup").asString();
					String cartifact = row.get("cartifact").asString();
					String cversion  = row.get("cversion").asString();
					int cyear        = row.get("cyear").asInt();
					String coords    = String.format("%s:%s:%s", lgroup, lartifact, lv1);

					String upgradeId = String.format("%s:%s:%s", coords, cgroup, cartifact);
					Upgrade latestVersion = latestClients.get(upgradeId);
					if (latestVersion != null && SemVerUtils.compare(latestVersion.cv, cversion) >= 0) {
						//logger.info("Skipping {} < {} for {}", cversion, latestVersion.cv, upgradeId);
						continue;
					} else {
						Map<String, String> fields = brokenReleases.get(coords);
						String lv2       = fields.get("v2");
						String level     = fields.get("level");
						int year         = Integer.parseInt(fields.get("year"));
						String jarV1     = fields.get("jar_v1");
						String jarV2     = fields.get("jar_v2");
						String delta     = fields.get("delta");

						Artifact client = downloader.downloadArtifact(new DefaultArtifact(cgroup, cartifact, "jar", cversion));
						if (client != null && client.getFile().exists()) {
							Upgrade upgrade = new Upgrade(lgroup, lartifact, lv1, lv2, level, year, jarV1, jarV2, cgroup, cartifact, cversion, cyear, delta);
							latestClients.put(upgradeId, upgrade);
						}
					}
				}

				for (String coords : latestClients.keySet()) {
					Upgrade upgrade = latestClients.get(coords);

					upgradesCsv.append(String.format("%s,%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%d,%s%n",
						upgrade.lgroup, upgrade.lartifact, upgrade.lv1, upgrade.lv2, upgrade.level,
						upgrade.year, upgrade.ljarV1, upgrade.ljarV2,
						upgrade.cgroup, upgrade.cartifact, upgrade.cv, upgrade.cyear, upgrade.delta));
				}
			}
		} catch (IOException e) {
			logger.error("IOE:", e);
		}
	}

	public void buildUpgradesDetectionsDatasets(float cochranE, float cochranP) {
		buildUpgradesDetectionsDataset(Paths.get(UPGRADES),       cochranE, cochranP);
		buildUpgradesDetectionsDataset(Paths.get(UPGRADES_MAJOR), cochranE, cochranP);
		buildUpgradesDetectionsDataset(Paths.get(UPGRADES_MINOR), cochranE, cochranP);
		buildUpgradesDetectionsDataset(Paths.get(UPGRADES_PATCH), cochranE, cochranP);
		buildUpgradesDetectionsDataset(Paths.get(UPGRADES_DEV),   cochranE, cochranP);
	}

	public void buildUpgradesDetectionsRaemaekersDatasets(float cochranE, float cochranP) {
		buildUpgradesDetectionsDataset(Paths.get(UPGRADES_RAEMAEKERS),       cochranE, cochranP);
		buildUpgradesDetectionsDataset(Paths.get(UPGRADES_MAJOR_RAEMAEKERS), cochranE, cochranP);
		buildUpgradesDetectionsDataset(Paths.get(UPGRADES_MINOR_RAEMAEKERS), cochranE, cochranP);
		buildUpgradesDetectionsDataset(Paths.get(UPGRADES_PATCH_RAEMAEKERS), cochranE, cochranP);
		buildUpgradesDetectionsDataset(Paths.get(UPGRADES_DEV_RAEMAEKERS),   cochranE, cochranP);
	}

	public void buildUpgradesDetectionsDataset(Path upgradesPath, float cochranE, float cochranP) {
		Path detectionsPath = upgradesPath.getParent().resolve("detections-" + upgradesPath.getFileName());
		File fd = detectionsPath.toFile();

		try (
			CSVReaderHeaderAware upgradesCsv = new CSVReaderHeaderAware(new FileReader(upgradesPath.toFile()));
			FileWriter detectionsCsv = new FileWriter(fd, true);
		) {
			logger.info("Building {}", detectionsPath);
			// Writing headers
			String[] suffixes = { "", "_stable", "_unstable", "_changed", "_breaking", "_nonbreaking", "_unused"};
			String[] metaArray = { "numDetections", "impactedTypes", "impactedMethods", "impactedFields" };
			String[] bcsArray = { "annotationDeprecatedAdded", "classRemoved", "classNowAbstract", "classNowFinal", "classNoLongerPublic",
					"classTypeChanged", "classNowCheckedException", "classLessAccessible", "superclassRemoved", "superclassAdded",
					"superclassModifiedIncompatible", "interfaceAdded", "interfaceRemoved", "methodRemoved", "methodRemovedInSuperclass",
					"methodLessAccessible", "methodLessAccessibleThanInSuperclass", "methodMoreAccessible", "methodIsStaticAndOverridesNotStatic",
					"methodReturnTypeChanged", "methodNowAbstract", "methodNowFinal", "methodNowStatic", "methodNoLongerStatic",
					"methodAddedToInterface", "methodAddedToPublicClass", "methodNowThrowsCheckedException", "methodAbstractAddedToClass",
					"methodAbstractAddedInSuperclass", "methodAbstractAddedInImplementedInterface", "methodNewDefault", "methodAbstractNowDefault",
					"fieldStaticAndOverridesStatic", "fieldLessAccessibleThanInSuperclass", "fieldNowFinal", "fieldNowStatic", "fieldNoLongerStatic",
					"fieldTypeChanged", "fieldRemoved", "fieldRemovedInSuperclass", "fieldLessAccessible", "fieldMoreAccessible",
					"constructorRemoved", "constructorLessAccessible" };

			if (fd.length() == 0) {
				String detectionsHeader = "cgroup,cartifact,cv,cyear,lgroup,lartifact,lv1,lv2,level,year,java_version,declarations,api_declarations,"; // Meta
				for (String suffix : suffixes) {
					if (suffix.equals("") || suffix.equals("_stable") || suffix.equals("_unstable")) {
						for (String meta : metaArray) {
							detectionsHeader += meta + suffix + ","; // Detections meta
						}
					}

					for (String bc : bcsArray) {
						detectionsHeader += bc + suffix + ","; // <Detections details>
					}
				}

				detectionsHeader += "delta,detection,exception,time\n"; // Closing data
				detectionsCsv.append(detectionsHeader);
				detectionsCsv.flush();
			}

			// Defining CSV format
			String format = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,"; // Meta
			String metaFormat = "%d,%d,%d,%d,"; // Detections meta
			String bcsFormat = "%d,%d,%d,%d,%d," // <Detections details>
					+ "%d,%d,%d,%d,"
					+ "%d,%d,%d,%d,"
					+ "%d,%d,%d,%d,%d,"
					+ "%d,%d,%d,%d,"
					+ "%d,%d,%d,%d,%d,"
					+ "%d,%d,%d,"
					+ "%d,%d,%d,%d,%d,%d,"
					+ "%d,%d,%d,%d,"
					+ "%d,%d,%d,%d,"; // </Detections details>

			for (String suffix : suffixes) {
				if (suffix.equals("") || suffix.equals("_stable") || suffix.equals("_unstable")) {
					format += metaFormat;
				}
				format += bcsFormat;
			}
			format += "%s,%s,%s,%d%n"; // Closing data
			String csvFormat = format;

			Map<String, String> line;
			Set<String> seen = new HashSet<>();
			AtomicInteger computed = new AtomicInteger(0);
			CSVReaderHeaderAware previous = new CSVReaderHeaderAware(new FileReader(fd));
			while ((line = previous.readMap()) != null) {
				String lgroup    = line.get("lgroup");
				String lartifact = line.get("lartifact");
				String lv1       = line.get("lv1");
				String lv2       = line.get("lv2");
				String cgroup    = line.get("cgroup");
				String cartifact = line.get("cartifact");
				String cv        = line.get("cv");
				String uuid      = String.format("%s:%s:%s:%s:%s:%s:%s",
					lgroup, lartifact, lv1, lv2, cgroup, cartifact, cv);

				seen.add(uuid);
				computed.incrementAndGet();
			}

			// Sampling
			List<Upgrade> upgrades = new ArrayList<>();
			while ((line = upgradesCsv.readMap()) != null) {
				Upgrade upgr = Upgrade.fromCsv(line);
				upgrades.add(upgr);
			}

			int sampleSize = new RScriptRunner(logger).sampleSize(upgrades.size(), cochranE, cochranP);
			logger.info("Sample size: {}", sampleSize);

			ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
			ConcurrentSoftReferenceObjectPool<Evaluator> pool = getEvaluatorPool();
			List<CompletableFuture<Object>> tasks = new ArrayList<>();
			int started = 0;
			while (started++ < upgrades.size()) {
				Random rand = new Random();
			    Upgrade upgrade = upgrades.remove(rand.nextInt(upgrades.size()));

			    String lgroup    = upgrade.lgroup;
				String lartifact = upgrade.lartifact;
				String lv1       = upgrade.lv1;
				String lv2       = upgrade.lv2;
				String level     = upgrade.level;
				int year         = upgrade.year;
				String ljarV1    = upgrade.ljarV1;
				String ljarV2    = upgrade.ljarV2;
				String cgroup    = upgrade.cgroup;
				String cartifact = upgrade.cartifact;
				String cv        = upgrade.cv;
				int cyear        = upgrade.cyear;
				String ldelta    = upgrade.delta;
				String uuid      = String.format("%s:%s:%s:%s:%s:%s:%s",
						lgroup, lartifact, lv1, lv2, cgroup, cartifact, cv);

				if (seen.contains(uuid))
					continue;

				tasks.add(CompletableFuture.supplyAsync(() -> {
					if (computed.get() > sampleSize)
						return null;

					Instant t1 = Instant.now();
					Artifact client = downloader.downloadArtifact(new DefaultArtifact(cgroup, cartifact, "jar", cv));

					if (client != null && client.getFile().exists()) {
						Path jar = client.getFile().toPath();
						JarStatistics jarStats = new JarStatistics();
						jarStats.compute(jar);

						boolean wrongVersion = jarStats.getJavaVersion() > 52;
						boolean wrongLanguage = !jarStats.getLanguage().equals("java");

						if (wrongVersion || wrongLanguage)
							return null; // No point losing time on the wrong versions

						logger.info("Computing detections for {}:{}:{} on Δ({}:{}, {} -> {})", cgroup, cartifact, cv, lgroup, lartifact, lv1, lv2);

						pool.useAndReturn((eval) -> {
							IValueFactory vf = eval.getValueFactory();

							Path jarV1 = Paths.get(ljarV1);
							Path jarV2 = Paths.get(ljarV2);
							Path deltaPath = Paths.get(ldelta);

							if (!jarV1.toString().isEmpty() && !jarV2.toString().isEmpty() && !deltaPath.toString().isEmpty()) {
								IValue delta = (IValue) eval.call("readBinaryDelta", vf.sourceLocation(ldelta));
								IValue m3V1 = (IValue) eval.call("createM3", vf.sourceLocation(jarV1.toAbsolutePath().toString()));
								IValue m3V2 = (IValue) eval.call("createM3", vf.sourceLocation(jarV2.toAbsolutePath().toString()));
								IValue m3Client = (IValue) eval.call("createM3", vf.sourceLocation(client.getFile().getAbsolutePath()));
								IValue evol = (IValue) eval.call("createEvolution", m3Client, m3V1, m3V2, delta);

								Path detectionsDir = Paths.get(DETECTIONS_PATH, lgroup, lartifact, String.format("%s_to_%s.delta", lv1, lv2),
										String.format("%s_%s_%s.detect", cgroup, cartifact, cv)).toAbsolutePath();
								new File(detectionsDir.toString()).getParentFile().mkdirs();

								ISet detections = (ISet) eval.call("computeDetections", m3Client, m3V1, m3V2, delta);

								IMap stats = (IMap) eval.call("detectionsStats", detections);
								IMap stableStats = (IMap) eval.call("stableDetectsStats", detections, delta);
								IMap unstableStats = (IMap) eval.call("unstableDetectsStats", detections, delta);
								IMap changedStats = (IMap) eval.call("changedStats", delta);
								IMap breakingStats = (IMap) eval.call("breakingStats", detections);
								IMap nonBreakingStats = (IMap) eval.call("nonBreakingStats", evol, detections);
								IMap unusedStats = (IMap) eval.call("unusedStats", evol, detections);

								Map<String, Object> values = createMapFromIMap(stats);
								Map<String, Object> stableValues = createMapFromIMap(stableStats);
								Map<String, Object> unstableValues = createMapFromIMap(unstableStats);
								Map<String, Object> changedValues = createMapFromIMap(changedStats);
								Map<String, Object> breakingValues = createMapFromIMap(breakingStats);
								Map<String, Object> nonBreakingValues = createMapFromIMap(nonBreakingStats);
								Map<String, Object> unusedValues = createMapFromIMap(unusedStats);

								int numDetects = getSafeInt(values, "numDetects", vf);
								int impactedTypes = getSafeInt(values, "impactedTypes", vf);
								int impactedMethods = getSafeInt(values, "impactedMethods", vf);
								int impactedFields = getSafeInt(values, "impactedFields", vf);

								int numDetectsStable = getSafeInt(stableValues, "numDetects", vf);
								int impactedTypesStable = getSafeInt(stableValues, "impactedTypes", vf);
								int impactedMethodsStable = getSafeInt(stableValues, "impactedMethods", vf);
								int impactedFieldsStable = getSafeInt(stableValues, "impactedFields", vf);

								int numDetectsUnstable = getSafeInt(unstableValues, "numDetects", vf);
								int impactedTypesUnstable = getSafeInt(unstableValues, "impactedTypes", vf);
								int impactedMethodsUnstable = getSafeInt(unstableValues, "impactedMethods", vf);
								int impactedFieldsUnstable = getSafeInt(unstableValues, "impactedFields", vf);

								long t = Duration.between(t1, Instant.now()).getSeconds();
								int current = computed.incrementAndGet();
								logger.info("[{}/{}] Computing detections for {}:{}:{} on Δ({}, {}, {} -> {}) took {}s",
									current, sampleSize, cgroup, cartifact, cv, lgroup, lartifact, lv1, lv2, t);

								synchronized (detectionsCsv) {
									try {
										detectionsCsv.append(String.format(csvFormat,
											cgroup, cartifact, cv, cyear, lgroup, lartifact, lv1, lv2, level, year,
											jarStats.getJavaVersion(), jarStats.getDeclarations(),jarStats.getAPIDeclarations(),
											numDetects, impactedTypes, impactedMethods, impactedFields,
											getSafeInt(values, "annotationDeprecatedAdded", vf),
											getSafeInt(values, "classRemoved", vf),
											getSafeInt(values, "classNowAbstract", vf),
											getSafeInt(values, "classNowFinal", vf),
											getSafeInt(values, "classNoLongerPublic", vf),
											getSafeInt(values, "classTypeChanged", vf),
											getSafeInt(values, "classNowCheckedException", vf),
											getSafeInt(values, "classLessAccessible", vf),
											getSafeInt(values, "superclassRemoved", vf),
											getSafeInt(values, "superclassAdded", vf),
											getSafeInt(values, "superclassModifiedIncompatible", vf),
											getSafeInt(values, "interfaceAdded", vf),
											getSafeInt(values, "interfaceRemoved", vf),
											getSafeInt(values, "methodRemoved", vf),
											getSafeInt(values, "methodRemovedInSuperclass", vf),
											getSafeInt(values, "methodLessAccessible", vf),
											getSafeInt(values, "methodLessAccessibleThanInSuperclass", vf),
											getSafeInt(values, "methodMoreAccessible", vf),
											getSafeInt(values, "methodIsStaticAndOverridesNotStatic", vf),
											getSafeInt(values, "methodReturnTypeChanged", vf),
											getSafeInt(values, "methodNowAbstract", vf),
											getSafeInt(values, "methodNowFinal", vf),
											getSafeInt(values, "methodNowStatic", vf),
											getSafeInt(values, "methodNoLongerStatic", vf),
											getSafeInt(values, "methodAddedToInterface", vf),
											getSafeInt(values, "methodAddedToPublicClass", vf),
											getSafeInt(values, "methodNowThrowsCheckedException", vf),
											getSafeInt(values, "methodAbstractAddedToClass", vf),
											getSafeInt(values, "methodAbstractAddedInSuperclass", vf),
											getSafeInt(values, "methodAbstractAddedInImplementedInterface", vf),
											getSafeInt(values, "methodNewDefault", vf),
											getSafeInt(values, "methodAbstractNowDefault", vf),
											getSafeInt(values, "fieldStaticAndOverridesStatic", vf),
											getSafeInt(values, "fieldLessAccessibleThanInSuperclass", vf),
											getSafeInt(values, "fieldNowFinal", vf),
											getSafeInt(values, "fieldNowStatic", vf),
											getSafeInt(values, "fieldNoLongerStatic", vf),
											getSafeInt(values, "fieldTypeChanged", vf),
											getSafeInt(values, "fieldRemoved", vf),
											getSafeInt(values, "fieldRemovedInSuperclass", vf),
											getSafeInt(values, "fieldLessAccessible", vf),
											getSafeInt(values, "fieldMoreAccessible", vf),
											getSafeInt(values, "constructorRemoved", vf),
											getSafeInt(values, "constructorLessAccessible", vf),
											// Stable detects
											numDetectsStable, impactedTypesStable, impactedMethodsStable, impactedFieldsStable,
											getSafeInt(stableValues, "annotationDeprecatedAdded", vf),
											getSafeInt(stableValues, "classRemoved", vf),
											getSafeInt(stableValues, "classNowAbstract", vf),
											getSafeInt(stableValues, "classNowFinal", vf),
											getSafeInt(stableValues, "classNoLongerPublic", vf),
											getSafeInt(stableValues, "classTypeChanged", vf),
											getSafeInt(stableValues, "classNowCheckedException", vf),
											getSafeInt(stableValues, "classLessAccessible", vf),
											getSafeInt(stableValues, "superclassRemoved", vf),
											getSafeInt(stableValues, "superclassAdded", vf),
											getSafeInt(stableValues, "superclassModifiedIncompatible", vf),
											getSafeInt(stableValues, "interfaceAdded", vf),
											getSafeInt(stableValues, "interfaceRemoved", vf),
											getSafeInt(stableValues, "methodRemoved", vf),
											getSafeInt(stableValues, "methodRemovedInSuperclass", vf),
											getSafeInt(stableValues, "methodLessAccessible", vf),
											getSafeInt(stableValues, "methodLessAccessibleThanInSuperclass", vf),
											getSafeInt(stableValues, "methodMoreAccessible", vf),
											getSafeInt(stableValues, "methodIsStaticAndOverridesNotStatic", vf),
											getSafeInt(stableValues, "methodReturnTypeChanged", vf),
											getSafeInt(stableValues, "methodNowAbstract", vf),
											getSafeInt(stableValues, "methodNowFinal", vf),
											getSafeInt(stableValues, "methodNowStatic", vf),
											getSafeInt(stableValues, "methodNoLongerStatic", vf),
											getSafeInt(stableValues, "methodAddedToInterface", vf),
											getSafeInt(stableValues, "methodAddedToPublicClass", vf),
											getSafeInt(stableValues, "methodNowThrowsCheckedException", vf),
											getSafeInt(stableValues, "methodAbstractAddedToClass", vf),
											getSafeInt(stableValues, "methodAbstractAddedInSuperclass", vf),
											getSafeInt(stableValues, "methodAbstractAddedInImplementedInterface", vf),
											getSafeInt(stableValues, "methodNewDefault", vf),
											getSafeInt(stableValues, "methodAbstractNowDefault", vf),
											getSafeInt(stableValues, "fieldStaticAndOverridesStatic", vf),
											getSafeInt(stableValues, "fieldLessAccessibleThanInSuperclass", vf),
											getSafeInt(stableValues, "fieldNowFinal", vf),
											getSafeInt(stableValues, "fieldNowStatic", vf),
											getSafeInt(stableValues, "fieldNoLongerStatic", vf),
											getSafeInt(stableValues, "fieldTypeChanged", vf),
											getSafeInt(stableValues, "fieldRemoved", vf),
											getSafeInt(stableValues, "fieldRemovedInSuperclass", vf),
											getSafeInt(stableValues, "fieldLessAccessible", vf),
											getSafeInt(stableValues, "fieldMoreAccessible", vf),
											getSafeInt(stableValues, "constructorRemoved", vf),
											getSafeInt(stableValues, "constructorLessAccessible", vf),
											// Unstable detects
											numDetectsUnstable, impactedTypesUnstable, impactedMethodsUnstable, impactedFieldsUnstable,
											getSafeInt(unstableValues, "annotationDeprecatedAdded", vf),
											getSafeInt(unstableValues, "classRemoved", vf),
											getSafeInt(unstableValues, "classNowAbstract", vf),
											getSafeInt(unstableValues, "classNowFinal", vf),
											getSafeInt(unstableValues, "classNoLongerPublic", vf),
											getSafeInt(unstableValues, "classTypeChanged", vf),
											getSafeInt(unstableValues, "classNowCheckedException", vf),
											getSafeInt(unstableValues, "classLessAccessible", vf),
											getSafeInt(unstableValues, "superclassRemoved", vf),
											getSafeInt(unstableValues, "superclassAdded", vf),
											getSafeInt(unstableValues, "superclassModifiedIncompatible", vf),
											getSafeInt(unstableValues, "interfaceAdded", vf),
											getSafeInt(unstableValues, "interfaceRemoved", vf),
											getSafeInt(unstableValues, "methodRemoved", vf),
											getSafeInt(unstableValues, "methodRemovedInSuperclass", vf),
											getSafeInt(unstableValues, "methodLessAccessible", vf),
											getSafeInt(unstableValues, "methodLessAccessibleThanInSuperclass", vf),
											getSafeInt(unstableValues, "methodMoreAccessible", vf),
											getSafeInt(unstableValues, "methodIsStaticAndOverridesNotStatic", vf),
											getSafeInt(unstableValues, "methodReturnTypeChanged", vf),
											getSafeInt(unstableValues, "methodNowAbstract", vf),
											getSafeInt(unstableValues, "methodNowFinal", vf),
											getSafeInt(unstableValues, "methodNowStatic", vf),
											getSafeInt(unstableValues, "methodNoLongerStatic", vf),
											getSafeInt(unstableValues, "methodAddedToInterface", vf),
											getSafeInt(unstableValues, "methodAddedToPublicClass", vf),
											getSafeInt(unstableValues, "methodNowThrowsCheckedException", vf),
											getSafeInt(unstableValues, "methodAbstractAddedToClass", vf),
											getSafeInt(unstableValues, "methodAbstractAddedInSuperclass", vf),
											getSafeInt(unstableValues, "methodAbstractAddedInImplementedInterface", vf),
											getSafeInt(unstableValues, "methodNewDefault", vf),
											getSafeInt(unstableValues, "methodAbstractNowDefault", vf),
											getSafeInt(unstableValues, "fieldStaticAndOverridesStatic", vf),
											getSafeInt(unstableValues, "fieldLessAccessibleThanInSuperclass", vf),
											getSafeInt(unstableValues, "fieldNowFinal", vf),
											getSafeInt(unstableValues, "fieldNowStatic", vf),
											getSafeInt(unstableValues, "fieldNoLongerStatic", vf),
											getSafeInt(unstableValues, "fieldTypeChanged", vf),
											getSafeInt(unstableValues, "fieldRemoved", vf),
											getSafeInt(unstableValues, "fieldRemovedInSuperclass", vf),
											getSafeInt(unstableValues, "fieldLessAccessible", vf),
											getSafeInt(unstableValues, "fieldMoreAccessible", vf),
											getSafeInt(unstableValues, "constructorRemoved", vf),
											getSafeInt(unstableValues, "constructorLessAccessible", vf),
											// Changed entities
											getSafeInt(changedValues, "annotationDeprecatedAdded", vf),
											getSafeInt(changedValues, "classRemoved", vf),
											getSafeInt(changedValues, "classNowAbstract", vf),
											getSafeInt(changedValues, "classNowFinal", vf),
											getSafeInt(changedValues, "classNoLongerPublic", vf),
											getSafeInt(changedValues, "classTypeChanged", vf),
											getSafeInt(changedValues, "classNowCheckedException", vf),
											getSafeInt(changedValues, "classLessAccessible", vf),
											getSafeInt(changedValues, "superclassRemoved", vf),
											getSafeInt(changedValues, "superclassAdded", vf),
											getSafeInt(changedValues, "superclassModifiedIncompatible", vf),
											getSafeInt(changedValues, "interfaceAdded", vf),
											getSafeInt(changedValues, "interfaceRemoved", vf),
											getSafeInt(changedValues, "methodRemoved", vf),
											getSafeInt(changedValues, "methodRemovedInSuperclass", vf),
											getSafeInt(changedValues, "methodLessAccessible", vf),
											getSafeInt(changedValues, "methodLessAccessibleThanInSuperclass", vf),
											getSafeInt(changedValues, "methodMoreAccessible", vf),
											getSafeInt(changedValues, "methodIsStaticAndOverridesNotStatic", vf),
											getSafeInt(changedValues, "methodReturnTypeChanged", vf),
											getSafeInt(changedValues, "methodNowAbstract", vf),
											getSafeInt(changedValues, "methodNowFinal", vf),
											getSafeInt(changedValues, "methodNowStatic", vf),
											getSafeInt(changedValues, "methodNoLongerStatic", vf),
											getSafeInt(changedValues, "methodAddedToInterface", vf),
											getSafeInt(changedValues, "methodAddedToPublicClass", vf),
											getSafeInt(changedValues, "methodNowThrowsCheckedException", vf),
											getSafeInt(changedValues, "methodAbstractAddedToClass", vf),
											getSafeInt(changedValues, "methodAbstractAddedInSuperclass", vf),
											getSafeInt(changedValues, "methodAbstractAddedInImplementedInterface", vf),
											getSafeInt(changedValues, "methodNewDefault", vf),
											getSafeInt(changedValues, "methodAbstractNowDefault", vf),
											getSafeInt(changedValues, "fieldStaticAndOverridesStatic", vf),
											getSafeInt(changedValues, "fieldLessAccessibleThanInSuperclass", vf),
											getSafeInt(changedValues, "fieldNowFinal", vf),
											getSafeInt(changedValues, "fieldNowStatic", vf),
											getSafeInt(changedValues, "fieldNoLongerStatic", vf),
											getSafeInt(changedValues, "fieldTypeChanged", vf),
											getSafeInt(changedValues, "fieldRemoved", vf),
											getSafeInt(changedValues, "fieldRemovedInSuperclass", vf),
											getSafeInt(changedValues, "fieldLessAccessible", vf),
											getSafeInt(changedValues, "fieldMoreAccessible", vf),
											getSafeInt(changedValues, "constructorRemoved", vf),
											getSafeInt(changedValues, "constructorLessAccessible", vf),
											// Breaking entities
											getSafeInt(breakingValues, "annotationDeprecatedAdded", vf),
											getSafeInt(breakingValues, "classRemoved", vf),
											getSafeInt(breakingValues, "classNowAbstract", vf),
											getSafeInt(breakingValues, "classNowFinal", vf),
											getSafeInt(breakingValues, "classNoLongerPublic", vf),
											getSafeInt(breakingValues, "classTypeChanged", vf),
											getSafeInt(breakingValues, "classNowCheckedException", vf),
											getSafeInt(breakingValues, "classLessAccessible", vf),
											getSafeInt(breakingValues, "superclassRemoved", vf),
											getSafeInt(breakingValues, "superclassAdded", vf),
											getSafeInt(breakingValues, "superclassModifiedIncompatible", vf),
											getSafeInt(breakingValues, "interfaceAdded", vf),
											getSafeInt(breakingValues, "interfaceRemoved", vf),
											getSafeInt(breakingValues, "methodRemoved", vf),
											getSafeInt(breakingValues, "methodRemovedInSuperclass", vf),
											getSafeInt(breakingValues, "methodLessAccessible", vf),
											getSafeInt(breakingValues, "methodLessAccessibleThanInSuperclass", vf),
											getSafeInt(breakingValues, "methodMoreAccessible", vf),
											getSafeInt(breakingValues, "methodIsStaticAndOverridesNotStatic", vf),
											getSafeInt(breakingValues, "methodReturnTypeChanged", vf),
											getSafeInt(breakingValues, "methodNowAbstract", vf),
											getSafeInt(breakingValues, "methodNowFinal", vf),
											getSafeInt(breakingValues, "methodNowStatic", vf),
											getSafeInt(breakingValues, "methodNoLongerStatic", vf),
											getSafeInt(breakingValues, "methodAddedToInterface", vf),
											getSafeInt(breakingValues, "methodAddedToPublicClass", vf),
											getSafeInt(breakingValues, "methodNowThrowsCheckedException", vf),
											getSafeInt(breakingValues, "methodAbstractAddedToClass", vf),
											getSafeInt(breakingValues, "methodAbstractAddedInSuperclass", vf),
											getSafeInt(breakingValues, "methodAbstractAddedInImplementedInterface", vf),
											getSafeInt(breakingValues, "methodNewDefault", vf),
											getSafeInt(breakingValues, "methodAbstractNowDefault", vf),
											getSafeInt(breakingValues, "fieldStaticAndOverridesStatic", vf),
											getSafeInt(breakingValues, "fieldLessAccessibleThanInSuperclass", vf),
											getSafeInt(breakingValues, "fieldNowFinal", vf),
											getSafeInt(breakingValues, "fieldNowStatic", vf),
											getSafeInt(breakingValues, "fieldNoLongerStatic", vf),
											getSafeInt(breakingValues, "fieldTypeChanged", vf),
											getSafeInt(breakingValues, "fieldRemoved", vf),
											getSafeInt(breakingValues, "fieldRemovedInSuperclass", vf),
											getSafeInt(breakingValues, "fieldLessAccessible", vf),
											getSafeInt(breakingValues, "fieldMoreAccessible", vf),
											getSafeInt(breakingValues, "constructorRemoved", vf),
											getSafeInt(breakingValues, "constructorLessAccessible", vf),
											// Non-breaking entities
											getSafeInt(nonBreakingValues, "annotationDeprecatedAdded", vf),
											getSafeInt(nonBreakingValues, "classRemoved", vf),
											getSafeInt(nonBreakingValues, "classNowAbstract", vf),
											getSafeInt(nonBreakingValues, "classNowFinal", vf),
											getSafeInt(nonBreakingValues, "classNoLongerPublic", vf),
											getSafeInt(nonBreakingValues, "classTypeChanged", vf),
											getSafeInt(nonBreakingValues, "classNowCheckedException", vf),
											getSafeInt(nonBreakingValues, "classLessAccessible", vf),
											getSafeInt(nonBreakingValues, "superclassRemoved", vf),
											getSafeInt(nonBreakingValues, "superclassAdded", vf),
											getSafeInt(nonBreakingValues, "superclassModifiedIncompatible", vf),
											getSafeInt(nonBreakingValues, "interfaceAdded", vf),
											getSafeInt(nonBreakingValues, "interfaceRemoved", vf),
											getSafeInt(nonBreakingValues, "methodRemoved", vf),
											getSafeInt(nonBreakingValues, "methodRemovedInSuperclass", vf),
											getSafeInt(nonBreakingValues, "methodLessAccessible", vf),
											getSafeInt(nonBreakingValues, "methodLessAccessibleThanInSuperclass", vf),
											getSafeInt(nonBreakingValues, "methodMoreAccessible", vf),
											getSafeInt(nonBreakingValues, "methodIsStaticAndOverridesNotStatic", vf),
											getSafeInt(nonBreakingValues, "methodReturnTypeChanged", vf),
											getSafeInt(nonBreakingValues, "methodNowAbstract", vf),
											getSafeInt(nonBreakingValues, "methodNowFinal", vf),
											getSafeInt(nonBreakingValues, "methodNowStatic", vf),
											getSafeInt(nonBreakingValues, "methodNoLongerStatic", vf),
											getSafeInt(nonBreakingValues, "methodAddedToInterface", vf),
											getSafeInt(nonBreakingValues, "methodAddedToPublicClass", vf),
											getSafeInt(nonBreakingValues, "methodNowThrowsCheckedException", vf),
											getSafeInt(nonBreakingValues, "methodAbstractAddedToClass", vf),
											getSafeInt(nonBreakingValues, "methodAbstractAddedInSuperclass", vf),
											getSafeInt(nonBreakingValues, "methodAbstractAddedInImplementedInterface", vf),
											getSafeInt(nonBreakingValues, "methodNewDefault", vf),
											getSafeInt(nonBreakingValues, "methodAbstractNowDefault", vf),
											getSafeInt(nonBreakingValues, "fieldStaticAndOverridesStatic", vf),
											getSafeInt(nonBreakingValues, "fieldLessAccessibleThanInSuperclass", vf),
											getSafeInt(nonBreakingValues, "fieldNowFinal", vf),
											getSafeInt(nonBreakingValues, "fieldNowStatic", vf),
											getSafeInt(nonBreakingValues, "fieldNoLongerStatic", vf),
											getSafeInt(nonBreakingValues, "fieldTypeChanged", vf),
											getSafeInt(nonBreakingValues, "fieldRemoved", vf),
											getSafeInt(nonBreakingValues, "fieldRemovedInSuperclass", vf),
											getSafeInt(nonBreakingValues, "fieldLessAccessible", vf),
											getSafeInt(nonBreakingValues, "fieldMoreAccessible", vf),
											getSafeInt(nonBreakingValues, "constructorRemoved", vf),
											getSafeInt(nonBreakingValues, "constructorLessAccessible", vf),
											// Unused entities
											getSafeInt(unusedValues, "annotationDeprecatedAdded", vf),
											getSafeInt(unusedValues, "classRemoved", vf),
											getSafeInt(unusedValues, "classNowAbstract", vf),
											getSafeInt(unusedValues, "classNowFinal", vf),
											getSafeInt(unusedValues, "classNoLongerPublic", vf),
											getSafeInt(unusedValues, "classTypeChanged", vf),
											getSafeInt(unusedValues, "classNowCheckedException", vf),
											getSafeInt(unusedValues, "classLessAccessible", vf),
											getSafeInt(unusedValues, "superclassRemoved", vf),
											getSafeInt(unusedValues, "superclassAdded", vf),
											getSafeInt(unusedValues, "superclassModifiedIncompatible", vf),
											getSafeInt(unusedValues, "interfaceAdded", vf),
											getSafeInt(unusedValues, "interfaceRemoved", vf),
											getSafeInt(unusedValues, "methodRemoved", vf),
											getSafeInt(unusedValues, "methodRemovedInSuperclass", vf),
											getSafeInt(unusedValues, "methodLessAccessible", vf),
											getSafeInt(unusedValues, "methodLessAccessibleThanInSuperclass", vf),
											getSafeInt(unusedValues, "methodMoreAccessible", vf),
											getSafeInt(unusedValues, "methodIsStaticAndOverridesNotStatic", vf),
											getSafeInt(unusedValues, "methodReturnTypeChanged", vf),
											getSafeInt(unusedValues, "methodNowAbstract", vf),
											getSafeInt(unusedValues, "methodNowFinal", vf),
											getSafeInt(unusedValues, "methodNowStatic", vf),
											getSafeInt(unusedValues, "methodNoLongerStatic", vf),
											getSafeInt(unusedValues, "methodAddedToInterface", vf),
											getSafeInt(unusedValues, "methodAddedToPublicClass", vf),
											getSafeInt(unusedValues, "methodNowThrowsCheckedException", vf),
											getSafeInt(unusedValues, "methodAbstractAddedToClass", vf),
											getSafeInt(unusedValues, "methodAbstractAddedInSuperclass", vf),
											getSafeInt(unusedValues, "methodAbstractAddedInImplementedInterface", vf),
											getSafeInt(unusedValues, "methodNewDefault", vf),
											getSafeInt(unusedValues, "methodAbstractNowDefault", vf),
											getSafeInt(unusedValues, "fieldStaticAndOverridesStatic", vf),
											getSafeInt(unusedValues, "fieldLessAccessibleThanInSuperclass", vf),
											getSafeInt(unusedValues, "fieldNowFinal", vf),
											getSafeInt(unusedValues, "fieldNowStatic", vf),
											getSafeInt(unusedValues, "fieldNoLongerStatic", vf),
											getSafeInt(unusedValues, "fieldTypeChanged", vf),
											getSafeInt(unusedValues, "fieldRemoved", vf),
											getSafeInt(unusedValues, "fieldRemovedInSuperclass", vf),
											getSafeInt(unusedValues, "fieldLessAccessible", vf),
											getSafeInt(unusedValues, "fieldMoreAccessible", vf),
											getSafeInt(unusedValues, "constructorRemoved", vf),
											getSafeInt(unusedValues, "constructorLessAccessible", vf),
											ldelta,
											detectionsDir.toAbsolutePath().toString().replace("\\", "/"), // Replace for Windows
											-1,
											t
										));
										detectionsCsv.flush();
									}
									catch (IOException e) {
										logger.error("IOE:", e);
									}
								}
							}
							else {
								logger.info("No info for {}:{} ({},{})", lgroup, lartifact, lv1, lv2);
							}
							return null;
						});
					}

					return null;
				}, executor).exceptionally(t -> {
					logger.error("Detection computation failed", t);
					return null;
				}));
			}

			CompletableFuture<Void> f = CompletableFuture.allOf(tasks.toArray(new CompletableFuture[tasks.size()]));
			f.join();
			logger.info("All futures completed successfully. {}", f);

			executor.shutdown();
			try {
				if (!executor.awaitTermination(5, TimeUnit.SECONDS))
					executor.shutdownNow();
			} catch (InterruptedException e) {
				executor.shutdownNow();
			}
		} catch (IOException e) {
			logger.error("IOE:", e);
		}
	}

	public void buildVersionsRaemaekersDataset(int clientGroupsPerLibrary) {
		try (
			Driver driver = GraphDatabase.driver(host, AuthTokens.basic(user, password));
			Session session = driver.session(AccessMode.READ);
			CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(LIBRARIES_RAEMAEKERS));
			FileWriter csv = new FileWriter(VERSIONS_RAEMAEKERS)
		) {
			csv.append("group,artifact,v1,v2,level,distance,age_diff,clients,client_groups,releases,age,activity,year\n");
			csv.flush();

			Map<String, Set<String>> libs = computeRaemaekersLibsMap(reader);
			int compared = 0;

			for (Map.Entry<String,Set<String>> entry : libs.entrySet()) {
				String[] id = entry.getKey().split(":");
				String group = id[0];
				String artifact = id[1];
				List<String> versions = new ArrayList<String>(entry.getValue());
				SemVerUtils.sortVersions(versions);

				for (int i = 0; i < versions.size(); i++) {
					if (i != versions.size() - 1) {
						String v1 = versions.get(i);
						String v2 = versions.get(i + 1);

						StatementResult result = session.run(
								"MATCH p=(a1:Artifact)-[:NEXT*]->(a2:Artifact) " +
								"WHERE a1.artifact = {a} " +
									"AND a1.groupID = {g} " +
									"AND a1.version = {v1} " +
									"AND a2.version = {v2} " +
									"AND a2.version =~ {r} " +
								"WITH p, a1, a2 " +

								"MATCH (c:Artifact)-[d:DEPENDS_ON]->(a1) " +
								"WHERE c.groupID <> a1.groupID " +
								//"AND d.scope IN [\"Compile\", \"Test\"]" +
								"WITH p, a1, a2, " +
									"size(collect(DISTINCT [c.groupID])) AS clientGroups, " +
									"size(collect(c)) AS clients " +
								"WHERE clientGroups > {n} " +
									"AND clients > {n} " +

								"UNWIND nodes(p) AS n " +

								"RETURN DISTINCT a1.groupID AS group, " +
									"a1.artifact AS artifact, " +
									"a1.version AS v1, " +
									"a2.version AS v2, " +
									"a1.packaging AS p1, " +
									"a2.packaging AS p2, " +
									"clients, " +
									"clientGroups, " +
									"duration.between(datetime(a1.release_date), datetime(a2.release_date)).days AS ageDiff, " +
									"length(p) AS releases, " +
									"datetime(a2.release_date).year AS year, " +
									"duration.between(min(datetime(n.release_date)), max(datetime(n.release_date))).months + 1 AS age",
								ImmutableMap.<String, Object>builder()
									.put("n", clientGroupsPerLibrary)
									.put("a", artifact)
									.put("g", group)
									.put("v1", v1)
									.put("v2", v2)
									.put("r", "(" + SemVerUtils.SEMVER_PATTERN + ")")
									.build()
							);

							if (result.hasNext()) {
								compared++;
							}

							appendResultVersionsDataset(csv, result);
					}
				}
			}

			logger.debug("Compared releases: {}", compared);

		}
		catch (IOException e) {
			logger.error("IOE:", e);
		}
	}

	private Map<String, Set<String>> computeRaemaekersLibsMap(CSVReaderHeaderAware reader) {
		Map<String, Set<String>> libs = new HashMap<String, Set<String>>(); // Key: group:artifact; Value: list of versions
		Map<String, String> line;

		try {
			while ((line = reader.readMap()) != null) {
				String group = line.get("group");
				String artifact = line.get("artifact");
				String version = line.get("version");

				String key = group + ":" + artifact;
				libs.computeIfAbsent(key, v -> new HashSet<String>());
				Set<String> versions = libs.get(key);
				versions.add(version);
				libs.put(key, versions);
			}
		}
		catch (IOException e) {
			logger.error("IOE:", e);
		}
		return libs;
	}

	/**
	 * versions.csv: list all library upgrades and their properties
	 *
	 * @param clientGroupsPerLibrary Only select library upgrades (v1, v2) for which v1
	 *        has clients from at least {@code clientGroupsPerLibrary} different groups
	 */
	public void buildVersionsDataset(int clientGroupsPerLibrary) {
		try (
			Driver driver = GraphDatabase.driver(host, AuthTokens.basic(user, password));
			Session session = driver.session(AccessMode.READ);
			FileWriter csv = new FileWriter(VERSIONS);
		) {
			csv.append("group,artifact,v1,v2,level,distance,age_diff,clients,client_groups,releases,age,activity,year\n");
			csv.flush();

			/**
			 * We're looking for adjacent JAR versions of the form X.Y[.Z].
			 * For every pair, we compute the number of releases up to this
			 * point, the age, the number of external clients and client groups,
			 * and the age difference between both versions.
			 */
			logger.info("Building versions dataset...");
			StatementResult result = session.run(
				"MATCH p=(l1:Artifact)-[:NEXT*]->(l2:Artifact) " +
				"WHERE l1.version =~ {r} " +
				"AND   l2.version =~ {r} " +
				"AND   none(n IN nodes(p)[1..-1] WHERE n.version =~ {r}) " +
				"WITH  l1, l2 " +

				"MATCH (c:Artifact)-[d:DEPENDS_ON]->(l1) " +
				"WHERE c.groupID <> l1.groupID " +
				//"AND d.scope IN [\"Compile\", \"Test\"]" +
				"WITH DISTINCT l1, l2, size(collect(DISTINCT [c.groupID])) AS clientGroups, size(collect(c)) AS clients " +
				"WHERE clientGroups > {n} " +
				"AND clients > {n} " +

				"MATCH p=(first:Artifact)-[:NEXT*]->(l2) " +
				"WHERE NOT ()-[:NEXT]->(first) " +

				"WITH " +
					"l1.groupID   AS group, " +
					"l1.artifact  AS artifact, " +
					"l1.version   AS v1, " +
					"l2.version   AS v2, " +
					"l1.packaging AS p1, " +
					"l2.packaging AS p2, " +
					"duration.between(datetime(l1.release_date), datetime(l2.release_date)).days AS ageDiff, " +
					"clients, " +
					"clientGroups, " +
					"length(p) AS releases, " +
					"datetime(l2.release_date).year AS year, " +
					"p " +

				"UNWIND nodes(p) AS n " +

				"RETURN " +
					"group, artifact, v1, v2, p1, p2, clients, clientGroups, ageDiff, releases, year, " +
					"duration.between(min(datetime(n.release_date)), max(datetime(n.release_date))).months + 1 AS age",
				ImmutableMap.of(
					"n", clientGroupsPerLibrary,
					//"p", "Jar", // For some reason, checking x.package takes age, so we just do it in Java below
					"r", "(" + SemVerUtils.SEMVER_PATTERN + ")"
				)
			);

			appendResultVersionsDataset(csv, result);
		}
		catch (IOException e) {
			logger.error("IOE:", e);
		}
	}

	private void appendResultVersionsDataset(FileWriter csv, StatementResult result) {
		while (result.hasNext()) {
			Record row = result.next();

			String group     = row.get("group").asString();
			String artifact  = row.get("artifact").asString();
			String v1        = row.get("v1").asString();
			String v2        = row.get("v2").asString();
			String p1        = row.get("p1").asString();
			String p2        = row.get("p2").asString();
			int clients      = row.get("clients").asInt();
			int clientGroups = row.get("clientGroups").asInt();
			int ageDiff      = row.get("ageDiff").asInt();
			int releases     = row.get("releases").asInt();
			int year         = row.get("year").asInt();
			int age          = row.get("age").asInt();
			double activity  = (double) releases / (double) age;
			SemInfo semInfo  = SemVerUtils.computeSemInfo(v1, v2);

			if (!p1.equals("Jar") || !p2.equals("Jar")) {
				logger.warn("Libraries not packaged as JARs. Skipping.");
				continue;
			}

			if (semInfo.level == SemLevel.UNKNOWN) {
				logger.warn("Invalid semantic versions <{}, {}>", v1, v2);
			}
			else {
				synchronized (csv) {
					logger.info("Valid upgrade {}:{} ({} -> {}) [{}]", group, artifact, v1, v2, semInfo.level);
					try {
						csv.append(String.format("%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,%f,%d%n",
							group, artifact, v1, v2, semInfo.level,	semInfo.distance, ageDiff,
							clients, clientGroups, releases, age, activity, year));
						csv.flush();
					}
					catch (IOException e) {
						logger.error("IOE:", e);
					}
				}
			}
		}
	}

	public void buildDeltasDataset() {
		buildDeltasDataset(VERSIONS, DELTAS);
	}

	public void buildDeltasRaemaekersDataset() {
		buildDeltasDataset(VERSIONS_RAEMAEKERS, DELTAS_RAEMAEKERS);
	}

	private void buildDeltasDataset(String versionsFile, String deltaFile) {
		File fd = new File(deltaFile);

		try (
			CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(versionsFile));
			FileWriter csv = new FileWriter(fd, true);
		) {
			if (fd.length() == 0)
				csv.append(
					"group,artifact,v1,v2,level,language,year,age_diff,distance,clients,client_groups,releases,age,activity," + // Meta
					"java_version_v1,java_version_v2,declarations_v1,api_declarations_v1,declarations_v2,api_declarations_v2," + // Code
					// Delta:
					"bcs,bin_compatible,src_compatible,expected_level,changes,added,removed,modified," + // Delta meta
					"broken_types,broken_methods,broken_fields,bcs_on_types,bcs_on_methods,bcs_on_fields," + // Delta meta
					"annotationDeprecatedAdded,classRemoved,classNowAbstract,classNowFinal,classNoLongerPublic," + // <Delta Details>
					"classTypeChanged,classNowCheckedException,classLessAccessible,superclassRemoved," +
					"superclassAdded,superclassModifiedIncompatible,interfaceAdded,interfaceRemoved," +
					"methodRemoved,methodRemovedInSuperclass,methodLessAccessible,methodLessAccessibleThanInSuperclass,methodMoreAccessible," +
					"methodIsStaticAndOverridesNotStatic,methodReturnTypeChanged,methodNowAbstract,methodNowFinal," +
					"methodNowStatic,methodNoLongerStatic,methodAddedToInterface,methodAddedToPublicClass,methodNowThrowsCheckedException," +
					"methodAbstractAddedToClass,methodAbstractAddedInSuperclass,methodAbstractAddedInImplementedInterface," +
					"methodNewDefault,methodAbstractNowDefault,fieldStaticAndOverridesStatic,fieldLessAccessibleThanInSuperclass," +
					"fieldNowFinal,fieldNowStatic,fieldNoLongerStatic,fieldTypeChanged,fieldRemoved,fieldRemovedInSuperclass," +
					"fieldLessAccessible,fieldMoreAccessible,constructorRemoved,constructorLessAccessible," + // </Delta details>
					// Stable delta:
					"bcs_stable,bin_compatible_stable,src_compatible_stable,expected_level_stable,changes_stable,added_stable,removed_stable,modified_stable," + // Delta meta
					"broken_types_stable,broken_methods_stable,broken_fields_stable,bcs_on_types_stable,bcs_on_methods_stable,bcs_on_fields_stable," + // Delta meta
					"annotationDeprecatedAdded_stable,classRemoved_stable,classNowAbstract_stable,classNowFinal_stable,classNoLongerPublic_stable," + // <Delta Details>
					"classTypeChanged_stable,classNowCheckedException_stable,classLessAccessible_stable,superclassRemoved_stable," +
					"superclassAdded_stable,superclassModifiedIncompatible_stable,interfaceAdded_stable,interfaceRemoved_stable," +
					"methodRemoved_stable,methodRemovedInSuperclass_stable,methodLessAccessible_stable,methodLessAccessibleThanInSuperclass_stable,methodMoreAccessible_stable," +
					"methodIsStaticAndOverridesNotStatic_stable,methodReturnTypeChanged_stable,methodNowAbstract_stable,methodNowFinal_stable," +
					"methodNowStatic_stable,methodNoLongerStatic_stable,methodAddedToInterface_stable,methodAddedToPublicClass_stable,methodNowThrowsCheckedException_stable," +
					"methodAbstractAddedToClass_stable,methodAbstractAddedInSuperclass_stable,methodAbstractAddedInImplementedInterface_stable," +
					"methodNewDefault_stable,methodAbstractNowDefault_stable,fieldStaticAndOverridesStatic_stable,fieldLessAccessibleThanInSuperclass_stable," +
					"fieldNowFinal_stable,fieldNowStatic_stable,fieldNoLongerStatic_stable,fieldTypeChanged_stable,fieldRemoved_stable,fieldRemovedInSuperclass_stable," +
					"fieldLessAccessible_stable,fieldMoreAccessible_stable,constructorRemoved_stable,constructorLessAccessible_stable," + // </Delta details>
					// Unstable delta:
					"bcs_unstable,bin_compatible_unstable,src_compatible_unstable,expected_level_unstable,changes_unstable,added_unstable,removed_unstable,modified_unstable," + // Delta meta
					"broken_types_unstable,broken_methods_unstable,broken_fields_unstable,bcs_on_types_unstable,bcs_on_methods_unstable,bcs_on_fields_unstable," + // Delta meta
					"annotationDeprecatedAdded_unstable,classRemoved_unstable,classNowAbstract_unstable,classNowFinal_unstable,classNoLongerPublic_unstable," + // <Delta Details>
					"classTypeChanged_unstable,classNowCheckedException_unstable,classLessAccessible_unstable,superclassRemoved_unstable," +
					"superclassAdded_unstable,superclassModifiedIncompatible_unstable,interfaceAdded_unstable,interfaceRemoved_unstable," +
					"methodRemoved_unstable,methodRemovedInSuperclass_unstable,methodLessAccessible_unstable,methodLessAccessibleThanInSuperclass_unstable,methodMoreAccessible_unstable," +
					"methodIsStaticAndOverridesNotStatic_unstable,methodReturnTypeChanged_unstable,methodNowAbstract_unstable,methodNowFinal_unstable," +
					"methodNowStatic_unstable,methodNoLongerStatic_unstable,methodAddedToInterface_unstable,methodAddedToPublicClass_unstable,methodNowThrowsCheckedException_unstable," +
					"methodAbstractAddedToClass_unstable,methodAbstractAddedInSuperclass_unstable,methodAbstractAddedInImplementedInterface_unstable," +
					"methodNewDefault_unstable,methodAbstractNowDefault_unstable,fieldStaticAndOverridesStatic_unstable,fieldLessAccessibleThanInSuperclass_unstable," +
					"fieldNowFinal_unstable,fieldNowStatic_unstable,fieldNoLongerStatic_unstable,fieldTypeChanged_unstable,fieldRemoved_unstable,fieldRemovedInSuperclass_unstable," +
					"fieldLessAccessible_unstable,fieldMoreAccessible_unstable,constructorRemoved_unstable,constructorLessAccessible_unstable," + // </Delta details>
					// Unstable pkg delta:
					"bcs_unstablePkg,bin_compatible_unstablePkg,src_compatible_unstablePkg,expected_level_unstablePkg,changes_unstablePkg,added_unstablePkg,removed_unstablePkg,modified_unstablePkg," + // Delta meta
					"broken_types_unstablePkg,broken_methods_unstablePkg,broken_fields_unstablePkg,bcs_on_types_unstablePkg,bcs_on_methods_unstablePkg,bcs_on_fields_unstablePkg," + // Delta meta
					"annotationDeprecatedAdded_unstablePkg,classRemoved_unstablePkg,classNowAbstract_unstablePkg,classNowFinal_unstablePkg,classNoLongerPublic_unstablePkg," + // <Delta Details>
					"classTypeChanged_unstablePkg,classNowCheckedException_unstablePkg,classLessAccessible_unstablePkg,superclassRemoved_unstablePkg," +
					"superclassAdded_unstablePkg,superclassModifiedIncompatible_unstablePkg,interfaceAdded_unstablePkg,interfaceRemoved_unstablePkg," +
					"methodRemoved_unstablePkg,methodRemovedInSuperclass_unstablePkg,methodLessAccessible_unstablePkg,methodLessAccessibleThanInSuperclass_unstablePkg,methodMoreAccessible_unstablePkg," +
					"methodIsStaticAndOverridesNotStatic_unstablePkg,methodReturnTypeChanged_unstablePkg,methodNowAbstract_unstablePkg,methodNowFinal_unstablePkg," +
					"methodNowStatic_unstablePkg,methodNoLongerStatic_unstablePkg,methodAddedToInterface_unstablePkg,methodAddedToPublicClass_unstablePkg,methodNowThrowsCheckedException_unstablePkg," +
					"methodAbstractAddedToClass_unstablePkg,methodAbstractAddedInSuperclass_unstablePkg,methodAbstractAddedInImplementedInterface_unstablePkg," +
					"methodNewDefault_unstablePkg,methodAbstractNowDefault_unstablePkg,fieldStaticAndOverridesStatic_unstablePkg,fieldLessAccessibleThanInSuperclass_unstablePkg," +
					"fieldNowFinal_unstablePkg,fieldNowStatic_unstablePkg,fieldNoLongerStatic_unstablePkg,fieldTypeChanged_unstablePkg,fieldRemoved_unstablePkg,fieldRemovedInSuperclass_unstablePkg," +
					"fieldLessAccessible_unstablePkg,fieldMoreAccessible_unstablePkg,constructorRemoved_unstablePkg,constructorLessAccessible_unstablePkg," + // </Delta details>
					// Unstable annon delta:
					"bcs_unstableAnnon,bin_compatible_unstableAnnon,src_compatible_unstableAnnon,expected_level_unstableAnnon,changes_unstableAnnon,added_unstableAnnon,removed_unstableAnnon,modified_unstableAnnon," + // Delta meta
					"broken_types_unstableAnnon,broken_methods_unstableAnnon,broken_fields_unstableAnnon,bcs_on_types_unstableAnnon,bcs_on_methods_unstableAnnon,bcs_on_fields_unstableAnnon," + // Delta meta
					"annotationDeprecatedAdded_unstableAnnon,classRemoved_unstableAnnon,classNowAbstract_unstableAnnon,classNowFinal_unstableAnnon,classNoLongerPublic_unstableAnnon," + // <Delta Details>
					"classTypeChanged_unstableAnnon,classNowCheckedException_unstableAnnon,classLessAccessible_unstableAnnon,superclassRemoved_unstableAnnon," +
					"superclassAdded_unstableAnnon,superclassModifiedIncompatible_unstableAnnon,interfaceAdded_unstableAnnon,interfaceRemoved_unstableAnnon," +
					"methodRemoved_unstableAnnon,methodRemovedInSuperclass_unstableAnnon,methodLessAccessible_unstableAnnon,methodLessAccessibleThanInSuperclass_unstableAnnon,methodMoreAccessible_unstableAnnon," +
					"methodIsStaticAndOverridesNotStatic_unstableAnnon,methodReturnTypeChanged_unstableAnnon,methodNowAbstract_unstableAnnon,methodNowFinal_unstableAnnon," +
					"methodNowStatic_unstableAnnon,methodNoLongerStatic_unstableAnnon,methodAddedToInterface_unstableAnnon,methodAddedToPublicClass_unstableAnnon,methodNowThrowsCheckedException_unstableAnnon," +
					"methodAbstractAddedToClass_unstableAnnon,methodAbstractAddedInSuperclass_unstableAnnon,methodAbstractAddedInImplementedInterface_unstableAnnon," +
					"methodNewDefault_unstableAnnon,methodAbstractNowDefault_unstableAnnon,fieldStaticAndOverridesStatic_unstableAnnon,fieldLessAccessibleThanInSuperclass_unstableAnnon," +
					"fieldNowFinal_unstableAnnon,fieldNowStatic_unstableAnnon,fieldNoLongerStatic_unstableAnnon,fieldTypeChanged_unstableAnnon,fieldRemoved_unstableAnnon,fieldRemovedInSuperclass_unstableAnnon," +
					"fieldLessAccessible_unstableAnnon,fieldMoreAccessible_unstableAnnon,constructorRemoved_unstableAnnon,constructorLessAccessible_unstableAnnon,unstableAnnons," + // </Delta details>
					"jar_v1,jar_v2,delta," +
					// Files
					"exception," +
					"t\n"
				);

			CSVReaderHeaderAware previous = new CSVReaderHeaderAware(new FileReader(fd));

			String csvFormat =
					"%s,%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%f," + // Meta
					"%d,%d,%d,%d,%d,%d," + // Code

					"%d,%s,%s,%s,%d,%d," + // Delta meta
					"%d,%d,%d,%d,%d,%d,%d,%d," + // Delta meta
					"%d,%d,%d,%d,%d," + // <Delta details>
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d,%d,%d," +

					"%d,%d,%d,%d," + // </Delta stable details>
					"%d,%s,%s,%s,%d,%d," + // Delta stable meta
					"%d,%d,%d,%d,%d,%d,%d,%d," + // Delta stable meta
					"%d,%d,%d,%d,%d," + // <Delta stable details>
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d,%d,%d," +
					"%d,%d,%d,%d," + // </Delta stable details>

					"%d,%s,%s,%s,%d,%d," + // Delta unstable meta
					"%d,%d,%d,%d,%d,%d,%d,%d," + // Delta unstable meta
					"%d,%d,%d,%d,%d," + // <Delta unstable details>
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d,%d,%d," +
					"%d,%d,%d,%d," + // </Delta unstable details>

					"%d,%s,%s,%s,%d,%d," + // Delta unstable pkg meta
					"%d,%d,%d,%d,%d,%d,%d,%d," + // Delta unstable pkg meta
					"%d,%d,%d,%d,%d," + // <Delta unstable pkg details>
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d,%d,%d," +
					"%d,%d,%d,%d," + // </Delta unstable pkg details>

					"%d,%s,%s,%s,%d,%d," + // Delta unstable annon meta
					"%d,%d,%d,%d,%d,%d,%d,%d," + // Delta unstable annon meta
					"%d,%d,%d,%d,%d," + // <Delta unstable annon details>
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d," +
					"%d,%d,%d,%d," +
					"%d,%d,%d,%d,%d,%d," +
					"%d,%d,%d,%d,%s," + // </Delta unstable annon details>

					"%s,%s,%s," + // Files
					"%s," +
					"%d%n";

			Map<String, String> line;
			Set<String> seen = new HashSet<>();
			while ((line = previous.readMap()) != null) {
				String group     = line.get("group");
				String artifact  = line.get("artifact");
				String v1        = line.get("v1");
				String v2        = line.get("v2");
				String uuid      = String.format("%s:%s:%s:%s", group, artifact, v1, v2);

				seen.add(uuid);
			}

			ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
			ConcurrentSoftReferenceObjectPool<Evaluator> pool = getEvaluatorPool();
			List<CompletableFuture<Object>> tasks = new ArrayList<>();
			while ((line = reader.readMap()) != null) {
				String group     = line.get("group");
				String artifact  = line.get("artifact");
				String v1        = line.get("v1");
				String v2        = line.get("v2");
				String level     = line.get("level");
				int year         = Integer.parseInt(line.get("year"));
				int distance     = Integer.parseInt(line.get("distance"));
				int ageDiff      = Integer.parseInt(line.get("age_diff"));
				int clients      = Integer.parseInt(line.get("clients"));
				int clientGroups = Integer.parseInt(line.get("client_groups"));
				int releases     = Integer.parseInt(line.get("releases"));
				int age          = Integer.parseInt(line.get("age"));
				double activity  = Double.parseDouble(line.get("activity"));
				String uuid      = String.format("%s:%s:%s:%s", group, artifact, v1, v2);

				if (seen.contains(uuid))
					continue;

				tasks.add(CompletableFuture.supplyAsync(() -> {
					Instant t1 = Instant.now();

					Artifact a1 = downloader.downloadArtifact(new DefaultArtifact(group, artifact, "jar", v1));
					Artifact a2 = downloader.downloadArtifact(new DefaultArtifact(group, artifact, "jar", v2));

					if (a1 != null && a1.getFile().exists() && a2 != null && a2.getFile().exists()) {
						Path jarV1 = a1.getFile().toPath();
						Path jarV2 = a2.getFile().toPath();
						JarStatistics statsV1 = new JarStatistics();
						JarStatistics statsV2 = new JarStatistics();
						statsV1.compute(jarV1);
						statsV2.compute(jarV2);

						boolean wrongVersion  = statsV1.getJavaVersion() > 52 || statsV2.getJavaVersion() > 52;
						boolean wrongLanguage = !statsV1.getLanguage().equals("java") || !statsV2.getLanguage().equals("java");

						if (wrongVersion || wrongLanguage) {
							synchronized (csv) {
								try {
									csv.append(String.format(csvFormat,
										group, artifact, v1, v2, level, statsV1.getLanguage(), year, distance, ageDiff, clients, clientGroups, releases, age, activity,
										statsV1.getJavaVersion(), statsV2.getJavaVersion(), statsV1.getDeclarations(),
										statsV1.getAPIDeclarations(), statsV2.getDeclarations(), statsV2.getAPIDeclarations(),
										// Delta
										-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
										-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
										-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1,
										// Stable delta
										-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
										-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
										-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1,
										// Unstable delta
										-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
										-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
										-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1,
										// Unstable pkg delta
										-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
										-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
										-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1,
										// Unstable annon delta
										-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
										-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
										-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1, "{}",
										-1, -1, -1,
										wrongVersion ? "invalid-java-version" : "invalid-language",
										Duration.between(t1, Instant.now()).getSeconds()
									));
								} catch (IOException e) {
									logger.error("IOE:", e);
								}
							}

							return null; // No point losing time
						}

						logger.info("Computing Δ({}, {}) on {}:{} [{}]", v1, v2, group, artifact, level);

						pool.useAndReturn((eval) -> {
							IValueFactory vf = eval.getValueFactory();
							Prelude prelude = new Prelude(vf, new PrintWriter(System.out), new PrintWriter(System.err));

							ISourceLocation locV1 = vf.sourceLocation(jarV1.toAbsolutePath().toString());
							ISourceLocation locV2 = vf.sourceLocation(jarV2.toAbsolutePath().toString());

							// Maracas delta and stats computation
							Path deltaPath = Paths.get(DELTAS_PATH, group, artifact, String.format("%s_to_%s.delta", v1, v2)).toAbsolutePath();
							new File(deltaPath.toString()).getParentFile().mkdirs();
							IValue delta;

							delta = (IValue) eval.call("compareJars", "org::maracas::delta::JApiCmp",
								ImmutableMap.of("oldCP", vf.list(), "newCP", vf.list()),
								locV1, locV2, vf.string(v1), vf.string(v2));
							prelude.writeBinaryValueFile(vf.sourceLocation(deltaPath.toString()), delta, vf.bool(false));

							IMap stats = (IMap) eval.call("deltaStats", delta);
							IMap stableStats = (IMap) eval.call("stableDeltaStats", delta);
							IMap unstableStats = (IMap) eval.call("unstableDeltaStats", delta);
							IMap unstablePkgStats = (IMap) eval.call("unstableDeltaPkgStats", delta);
							IMap unstableAnnonStats = (IMap) eval.call("unstableDeltaAnnonStats", delta);

							Map<String, Object> values = createMapFromIMap(stats);
							Map<String, Object> stableValues = createMapFromIMap(stableStats);
							Map<String, Object> unstableValues = createMapFromIMap(unstableStats);
							Map<String, Object> unstablePkgValues = createMapFromIMap(unstablePkgStats);
							Map<String, Object> unstableAnnonValues = createMapFromIMap(unstableAnnonStats);

							int bcs = getSafeInt(values, "bcs", vf);
							int changes = getSafeInt(values, "changes", vf);
							int added = getSafeInt(values, "added", vf);
							int removed = getSafeInt(values, "removed", vf);
							int modified = getSafeInt(values, "modified", vf);
							int deprecated = getSafeInt(values, "deprecated", vf);
							SemLevel expected = SemVerUtils.computeExpectedLevel(bcs, added, modified, deprecated);

							int bcsStable = getSafeInt(stableValues, "bcs", vf);
							int changesStable = getSafeInt(stableValues, "changes", vf);
							int addedStable = getSafeInt(stableValues, "added", vf);
							int removedStable = getSafeInt(stableValues, "removed", vf);
							int modifiedStable = getSafeInt(stableValues, "modified", vf);
							int deprecatedStable = getSafeInt(stableValues, "deprecated", vf);
							SemLevel expectedStable = SemVerUtils.computeExpectedLevel(bcsStable, addedStable, modifiedStable, deprecatedStable);

							int bcsUnstable = getSafeInt(unstableValues, "bcs", vf);
							int changesUnstable = getSafeInt(unstableValues, "changes", vf);
							int addedUnstable = getSafeInt(unstableValues, "added", vf);
							int removedUnstable = getSafeInt(unstableValues, "removed", vf);
							int modifiedUnstable = getSafeInt(unstableValues, "modified", vf);
							int deprecatedUnstable = getSafeInt(unstableValues, "deprecated", vf);
							SemLevel expectedUnstable = SemVerUtils.computeExpectedLevel(bcsUnstable, addedUnstable, modifiedUnstable, deprecatedUnstable);

							int bcsUnstablePkg = getSafeInt(unstablePkgValues, "bcs", vf);
							int changesUnstablePkg = getSafeInt(unstablePkgValues, "changes", vf);
							int addedUnstablePkg = getSafeInt(unstablePkgValues, "added", vf);
							int removedUnstablePkg = getSafeInt(unstablePkgValues, "removed", vf);
							int modifiedUnstablePkg = getSafeInt(unstablePkgValues, "modified", vf);
							int deprecatedUnstablePkg = getSafeInt(unstablePkgValues, "deprecated", vf);
							SemLevel expectedUnstablePkg = SemVerUtils.computeExpectedLevel(bcsUnstablePkg, addedUnstablePkg, modifiedUnstablePkg, deprecatedUnstablePkg);

							int bcsUnstableAnnon = getSafeInt(unstableAnnonValues, "bcs", vf);
							int changesUnstableAnnon = getSafeInt(unstableAnnonValues, "changes", vf);
							int addedUnstableAnnon = getSafeInt(unstableAnnonValues, "added", vf);
							int removedUnstableAnnon = getSafeInt(unstableAnnonValues, "removed", vf);
							int modifiedUnstableAnnon = getSafeInt(unstableAnnonValues, "modified", vf);
							int deprecatedUnstableAnnon = getSafeInt(unstableAnnonValues, "deprecated", vf);
							SemLevel expectedUnstableAnnon = SemVerUtils.computeExpectedLevel(bcsUnstableAnnon, addedUnstableAnnon, modifiedUnstableAnnon, deprecatedUnstableAnnon);
							String unstableAnnons = ((ISet) eval.call("getUnstableAnnons", delta)).toString();

							long t = Duration.between(t1, Instant.now()).getSeconds();
							logger.info("Δ({}, {}) on {}:{} [{}] took {}s", v1, v2, group, artifact, level, t);

							synchronized (csv) {
								try {
									csv.append(String.format(csvFormat,
										group, artifact, v1, v2, level, statsV1.getLanguage(), year, ageDiff, distance, clients, clientGroups, releases, age, activity,
										statsV1.getJavaVersion(), statsV2.getJavaVersion(), statsV1.getDeclarations(),
										statsV1.getAPIDeclarations(), statsV2.getDeclarations(), statsV2.getAPIDeclarations(),
										// Delta
										bcs,
										getSafeBool(values, "binaryCompatible", vf),
										getSafeBool(values, "sourceCompatible", vf),
										expected,
										changes,
										added,
										removed,
										modified,
										getSafeInt(values, "brokenTypes", vf),
										getSafeInt(values, "brokenMethods", vf),
										getSafeInt(values, "brokenFields", vf),
										getSafeInt(values, "bcsOnTypes", vf),
										getSafeInt(values, "bcsOnMethods", vf),
										getSafeInt(values, "bcsOnFields", vf),
										deprecated,
										getSafeInt(values, "classRemoved", vf),
										getSafeInt(values, "classNowAbstract", vf),
										getSafeInt(values, "classNowFinal", vf),
										getSafeInt(values, "classNoLongerPublic", vf),
										getSafeInt(values, "classTypeChanged", vf),
										getSafeInt(values, "classNowCheckedException", vf),
										getSafeInt(values, "classLessAccessible", vf),
										getSafeInt(values, "superclassRemoved", vf),
										getSafeInt(values, "superclassAdded", vf),
										getSafeInt(values, "superclassModifiedIncompatible", vf),
										getSafeInt(values, "interfaceAdded", vf),
										getSafeInt(values, "interfaceRemoved", vf),
										getSafeInt(values, "methodRemoved", vf),
										getSafeInt(values, "methodRemovedInSuperclass", vf),
										getSafeInt(values, "methodLessAccessible", vf),
										getSafeInt(values, "methodLessAccessibleThanInSuperclass", vf),
										getSafeInt(values, "methodMoreAccessible", vf),
										getSafeInt(values, "methodIsStaticAndOverridesNotStatic", vf),
										getSafeInt(values, "methodReturnTypeChanged", vf),
										getSafeInt(values, "methodNowAbstract", vf),
										getSafeInt(values, "methodNowFinal", vf),
										getSafeInt(values, "methodNowStatic", vf),
										getSafeInt(values, "methodNoLongerStatic", vf),
										getSafeInt(values, "methodAddedToInterface", vf),
										getSafeInt(values, "methodAddedToPublicClass", vf),
										getSafeInt(values, "methodNowThrowsCheckedException", vf),
										getSafeInt(values, "methodAbstractAddedToClass", vf),
										getSafeInt(values, "methodAbstractAddedInSuperclass", vf),
										getSafeInt(values, "methodAbstractAddedInImplementedInterface", vf),
										getSafeInt(values, "methodNewDefault", vf),
										getSafeInt(values, "methodAbstractNowDefault", vf),
										getSafeInt(values, "fieldStaticAndOverridesStatic", vf),
										getSafeInt(values, "fieldLessAccessibleThanInSuperclass", vf),
										getSafeInt(values, "fieldNowFinal", vf),
										getSafeInt(values, "fieldNowStatic", vf),
										getSafeInt(values, "fieldNoLongerStatic", vf),
										getSafeInt(values, "fieldTypeChanged", vf),
										getSafeInt(values, "fieldRemoved", vf),
										getSafeInt(values, "fieldRemovedInSuperclass", vf),
										getSafeInt(values, "fieldLessAccessible", vf),
										getSafeInt(values, "fieldMoreAccessible", vf),
										getSafeInt(values, "constructorRemoved", vf),
										getSafeInt(values, "constructorLessAccessible", vf),
										// Stable delta
										bcsStable,
										getSafeBool(stableValues, "binaryCompatible", vf),
										getSafeBool(stableValues, "sourceCompatible", vf),
										expectedStable,
										changesStable,
										addedStable,
										removedStable,
										modifiedStable,
										getSafeInt(stableValues, "brokenTypes", vf),
										getSafeInt(stableValues, "brokenMethods", vf),
										getSafeInt(stableValues, "brokenFields", vf),
										getSafeInt(stableValues, "bcsOnTypes", vf),
										getSafeInt(stableValues, "bcsOnMethods", vf),
										getSafeInt(stableValues, "bcsOnFields", vf),
										deprecatedStable,
										getSafeInt(stableValues, "classRemoved", vf),
										getSafeInt(stableValues, "classNowAbstract", vf),
										getSafeInt(stableValues, "classNowFinal", vf),
										getSafeInt(stableValues, "classNoLongerPublic", vf),
										getSafeInt(stableValues, "classTypeChanged", vf),
										getSafeInt(stableValues, "classNowCheckedException", vf),
										getSafeInt(stableValues, "classLessAccessible", vf),
										getSafeInt(stableValues, "superclassRemoved", vf),
										getSafeInt(stableValues, "superclassAdded", vf),
										getSafeInt(stableValues, "superclassModifiedIncompatible", vf),
										getSafeInt(stableValues, "interfaceAdded", vf),
										getSafeInt(stableValues, "interfaceRemoved", vf),
										getSafeInt(stableValues, "methodRemoved", vf),
										getSafeInt(stableValues, "methodRemovedInSuperclass", vf),
										getSafeInt(stableValues, "methodLessAccessible", vf),
										getSafeInt(stableValues, "methodLessAccessibleThanInSuperclass", vf),
										getSafeInt(stableValues, "methodMoreAccessible", vf),
										getSafeInt(stableValues, "methodIsStaticAndOverridesNotStatic", vf),
										getSafeInt(stableValues, "methodReturnTypeChanged", vf),
										getSafeInt(stableValues, "methodNowAbstract", vf),
										getSafeInt(stableValues, "methodNowFinal", vf),
										getSafeInt(stableValues, "methodNowStatic", vf),
										getSafeInt(stableValues, "methodNoLongerStatic", vf),
										getSafeInt(stableValues, "methodAddedToInterface", vf),
										getSafeInt(stableValues, "methodAddedToPublicClass", vf),
										getSafeInt(stableValues, "methodNowThrowsCheckedException", vf),
										getSafeInt(stableValues, "methodAbstractAddedToClass", vf),
										getSafeInt(stableValues, "methodAbstractAddedInSuperclass", vf),
										getSafeInt(stableValues, "methodAbstractAddedInImplementedInterface", vf),
										getSafeInt(stableValues, "methodNewDefault", vf),
										getSafeInt(stableValues, "methodAbstractNowDefault", vf),
										getSafeInt(stableValues, "fieldStaticAndOverridesStatic", vf),
										getSafeInt(stableValues, "fieldLessAccessibleThanInSuperclass", vf),
										getSafeInt(stableValues, "fieldNowFinal", vf),
										getSafeInt(stableValues, "fieldNowStatic", vf),
										getSafeInt(stableValues, "fieldNoLongerStatic", vf),
										getSafeInt(stableValues, "fieldTypeChanged", vf),
										getSafeInt(stableValues, "fieldRemoved", vf),
										getSafeInt(stableValues, "fieldRemovedInSuperclass", vf),
										getSafeInt(stableValues, "fieldLessAccessible", vf),
										getSafeInt(stableValues, "fieldMoreAccessible", vf),
										getSafeInt(stableValues, "constructorRemoved", vf),
										getSafeInt(stableValues, "constructorLessAccessible", vf),
										// Unstable delta
										bcsUnstable,
										getSafeBool(unstableValues, "binaryCompatible", vf),
										getSafeBool(unstableValues, "sourceCompatible", vf),
										expectedUnstable,
										changesUnstable,
										addedUnstable,
										removedUnstable,
										modifiedUnstable,
										getSafeInt(unstableValues, "brokenTypes", vf),
										getSafeInt(unstableValues, "brokenMethods", vf),
										getSafeInt(unstableValues, "brokenFields", vf),
										getSafeInt(unstableValues, "bcsOnTypes", vf),
										getSafeInt(unstableValues, "bcsOnMethods", vf),
										getSafeInt(unstableValues, "bcsOnFields", vf),
										deprecatedUnstable,
										getSafeInt(unstableValues, "classRemoved", vf),
										getSafeInt(unstableValues, "classNowAbstract", vf),
										getSafeInt(unstableValues, "classNowFinal", vf),
										getSafeInt(unstableValues, "classNoLongerPublic", vf),
										getSafeInt(unstableValues, "classTypeChanged", vf),
										getSafeInt(unstableValues, "classNowCheckedException", vf),
										getSafeInt(unstableValues, "classLessAccessible", vf),
										getSafeInt(unstableValues, "superclassRemoved", vf),
										getSafeInt(unstableValues, "superclassAdded", vf),
										getSafeInt(unstableValues, "superclassModifiedIncompatible", vf),
										getSafeInt(unstableValues, "interfaceAdded", vf),
										getSafeInt(unstableValues, "interfaceRemoved", vf),
										getSafeInt(unstableValues, "methodRemoved", vf),
										getSafeInt(unstableValues, "methodRemovedInSuperclass", vf),
										getSafeInt(unstableValues, "methodLessAccessible", vf),
										getSafeInt(unstableValues, "methodLessAccessibleThanInSuperclass", vf),
										getSafeInt(unstableValues, "methodMoreAccessible", vf),
										getSafeInt(unstableValues, "methodIsStaticAndOverridesNotStatic", vf),
										getSafeInt(unstableValues, "methodReturnTypeChanged", vf),
										getSafeInt(unstableValues, "methodNowAbstract", vf),
										getSafeInt(unstableValues, "methodNowFinal", vf),
										getSafeInt(unstableValues, "methodNowStatic", vf),
										getSafeInt(unstableValues, "methodNoLongerStatic", vf),
										getSafeInt(unstableValues, "methodAddedToInterface", vf),
										getSafeInt(unstableValues, "methodAddedToPublicClass", vf),
										getSafeInt(unstableValues, "methodNowThrowsCheckedException", vf),
										getSafeInt(unstableValues, "methodAbstractAddedToClass", vf),
										getSafeInt(unstableValues, "methodAbstractAddedInSuperclass", vf),
										getSafeInt(unstableValues, "methodAbstractAddedInImplementedInterface", vf),
										getSafeInt(unstableValues, "methodNewDefault", vf),
										getSafeInt(unstableValues, "methodAbstractNowDefault", vf),
										getSafeInt(unstableValues, "fieldStaticAndOverridesStatic", vf),
										getSafeInt(unstableValues, "fieldLessAccessibleThanInSuperclass", vf),
										getSafeInt(unstableValues, "fieldNowFinal", vf),
										getSafeInt(unstableValues, "fieldNowStatic", vf),
										getSafeInt(unstableValues, "fieldNoLongerStatic", vf),
										getSafeInt(unstableValues, "fieldTypeChanged", vf),
										getSafeInt(unstableValues, "fieldRemoved", vf),
										getSafeInt(unstableValues, "fieldRemovedInSuperclass", vf),
										getSafeInt(unstableValues, "fieldLessAccessible", vf),
										getSafeInt(unstableValues, "fieldMoreAccessible", vf),
										getSafeInt(unstableValues, "constructorRemoved", vf),
										getSafeInt(unstableValues, "constructorLessAccessible", vf),
										// Unstable pkg delta
										bcsUnstablePkg,
										getSafeBool(unstablePkgValues, "binaryCompatible", vf),
										getSafeBool(unstablePkgValues, "sourceCompatible", vf),
										expectedUnstablePkg,
										changesUnstablePkg,
										addedUnstablePkg,
										removedUnstablePkg,
										modifiedUnstablePkg,
										getSafeInt(unstablePkgValues, "brokenTypes", vf),
										getSafeInt(unstablePkgValues, "brokenMethods", vf),
										getSafeInt(unstablePkgValues, "brokenFields", vf),
										getSafeInt(unstablePkgValues, "bcsOnTypes", vf),
										getSafeInt(unstablePkgValues, "bcsOnMethods", vf),
										getSafeInt(unstablePkgValues, "bcsOnFields", vf),
										deprecatedUnstablePkg,
										getSafeInt(unstablePkgValues, "classRemoved", vf),
										getSafeInt(unstablePkgValues, "classNowAbstract", vf),
										getSafeInt(unstablePkgValues, "classNowFinal", vf),
										getSafeInt(unstablePkgValues, "classNoLongerPublic", vf),
										getSafeInt(unstablePkgValues, "classTypeChanged", vf),
										getSafeInt(unstablePkgValues, "classNowCheckedException", vf),
										getSafeInt(unstablePkgValues, "classLessAccessible", vf),
										getSafeInt(unstablePkgValues, "superclassRemoved", vf),
										getSafeInt(unstablePkgValues, "superclassAdded", vf),
										getSafeInt(unstablePkgValues, "superclassModifiedIncompatible", vf),
										getSafeInt(unstablePkgValues, "interfaceAdded", vf),
										getSafeInt(unstablePkgValues, "interfaceRemoved", vf),
										getSafeInt(unstablePkgValues, "methodRemoved", vf),
										getSafeInt(unstablePkgValues, "methodRemovedInSuperclass", vf),
										getSafeInt(unstablePkgValues, "methodLessAccessible", vf),
										getSafeInt(unstablePkgValues, "methodLessAccessibleThanInSuperclass", vf),
										getSafeInt(unstablePkgValues, "methodMoreAccessible", vf),
										getSafeInt(unstablePkgValues, "methodIsStaticAndOverridesNotStatic", vf),
										getSafeInt(unstablePkgValues, "methodReturnTypeChanged", vf),
										getSafeInt(unstablePkgValues, "methodNowAbstract", vf),
										getSafeInt(unstablePkgValues, "methodNowFinal", vf),
										getSafeInt(unstablePkgValues, "methodNowStatic", vf),
										getSafeInt(unstablePkgValues, "methodNoLongerStatic", vf),
										getSafeInt(unstablePkgValues, "methodAddedToInterface", vf),
										getSafeInt(unstablePkgValues, "methodAddedToPublicClass", vf),
										getSafeInt(unstablePkgValues, "methodNowThrowsCheckedException", vf),
										getSafeInt(unstablePkgValues, "methodAbstractAddedToClass", vf),
										getSafeInt(unstablePkgValues, "methodAbstractAddedInSuperclass", vf),
										getSafeInt(unstablePkgValues, "methodAbstractAddedInImplementedInterface", vf),
										getSafeInt(unstablePkgValues, "methodNewDefault", vf),
										getSafeInt(unstablePkgValues, "methodAbstractNowDefault", vf),
										getSafeInt(unstablePkgValues, "fieldStaticAndOverridesStatic", vf),
										getSafeInt(unstablePkgValues, "fieldLessAccessibleThanInSuperclass", vf),
										getSafeInt(unstablePkgValues, "fieldNowFinal", vf),
										getSafeInt(unstablePkgValues, "fieldNowStatic", vf),
										getSafeInt(unstablePkgValues, "fieldNoLongerStatic", vf),
										getSafeInt(unstablePkgValues, "fieldTypeChanged", vf),
										getSafeInt(unstablePkgValues, "fieldRemoved", vf),
										getSafeInt(unstablePkgValues, "fieldRemovedInSuperclass", vf),
										getSafeInt(unstablePkgValues, "fieldLessAccessible", vf),
										getSafeInt(unstablePkgValues, "fieldMoreAccessible", vf),
										getSafeInt(unstablePkgValues, "constructorRemoved", vf),
										getSafeInt(unstablePkgValues, "constructorLessAccessible", vf),
										// Unstable annon delta
										bcsUnstableAnnon,
										getSafeBool(unstableAnnonValues, "binaryCompatible", vf),
										getSafeBool(unstableAnnonValues, "sourceCompatible", vf),
										expectedUnstableAnnon,
										changesUnstableAnnon,
										addedUnstableAnnon,
										removedUnstableAnnon,
										modifiedUnstableAnnon,
										getSafeInt(unstableAnnonValues, "brokenTypes", vf),
										getSafeInt(unstableAnnonValues, "brokenMethods", vf),
										getSafeInt(unstableAnnonValues, "brokenFields", vf),
										getSafeInt(unstableAnnonValues, "bcsOnTypes", vf),
										getSafeInt(unstableAnnonValues, "bcsOnMethods", vf),
										getSafeInt(unstableAnnonValues, "bcsOnFields", vf),
										deprecatedUnstableAnnon,
										getSafeInt(unstableAnnonValues, "classRemoved", vf),
										getSafeInt(unstableAnnonValues, "classNowAbstract", vf),
										getSafeInt(unstableAnnonValues, "classNowFinal", vf),
										getSafeInt(unstableAnnonValues, "classNoLongerPublic", vf),
										getSafeInt(unstableAnnonValues, "classTypeChanged", vf),
										getSafeInt(unstableAnnonValues, "classNowCheckedException", vf),
										getSafeInt(unstableAnnonValues, "classLessAccessible", vf),
										getSafeInt(unstableAnnonValues, "superclassRemoved", vf),
										getSafeInt(unstableAnnonValues, "superclassAdded", vf),
										getSafeInt(unstableAnnonValues, "superclassModifiedIncompatible", vf),
										getSafeInt(unstableAnnonValues, "interfaceAdded", vf),
										getSafeInt(unstableAnnonValues, "interfaceRemoved", vf),
										getSafeInt(unstableAnnonValues, "methodRemoved", vf),
										getSafeInt(unstableAnnonValues, "methodRemovedInSuperclass", vf),
										getSafeInt(unstableAnnonValues, "methodLessAccessible", vf),
										getSafeInt(unstableAnnonValues, "methodLessAccessibleThanInSuperclass", vf),
										getSafeInt(unstableAnnonValues, "methodMoreAccessible", vf),
										getSafeInt(unstableAnnonValues, "methodIsStaticAndOverridesNotStatic", vf),
										getSafeInt(unstableAnnonValues, "methodReturnTypeChanged", vf),
										getSafeInt(unstableAnnonValues, "methodNowAbstract", vf),
										getSafeInt(unstableAnnonValues, "methodNowFinal", vf),
										getSafeInt(unstableAnnonValues, "methodNowStatic", vf),
										getSafeInt(unstableAnnonValues, "methodNoLongerStatic", vf),
										getSafeInt(unstableAnnonValues, "methodAddedToInterface", vf),
										getSafeInt(unstableAnnonValues, "methodAddedToPublicClass", vf),
										getSafeInt(unstableAnnonValues, "methodNowThrowsCheckedException", vf),
										getSafeInt(unstableAnnonValues, "methodAbstractAddedToClass", vf),
										getSafeInt(unstableAnnonValues, "methodAbstractAddedInSuperclass", vf),
										getSafeInt(unstableAnnonValues, "methodAbstractAddedInImplementedInterface", vf),
										getSafeInt(unstableAnnonValues, "methodNewDefault", vf),
										getSafeInt(unstableAnnonValues, "methodAbstractNowDefault", vf),
										getSafeInt(unstableAnnonValues, "fieldStaticAndOverridesStatic", vf),
										getSafeInt(unstableAnnonValues, "fieldLessAccessibleThanInSuperclass", vf),
										getSafeInt(unstableAnnonValues, "fieldNowFinal", vf),
										getSafeInt(unstableAnnonValues, "fieldNowStatic", vf),
										getSafeInt(unstableAnnonValues, "fieldNoLongerStatic", vf),
										getSafeInt(unstableAnnonValues, "fieldTypeChanged", vf),
										getSafeInt(unstableAnnonValues, "fieldRemoved", vf),
										getSafeInt(unstableAnnonValues, "fieldRemovedInSuperclass", vf),
										getSafeInt(unstableAnnonValues, "fieldLessAccessible", vf),
										getSafeInt(unstableAnnonValues, "fieldMoreAccessible", vf),
										getSafeInt(unstableAnnonValues, "constructorRemoved", vf),
										getSafeInt(unstableAnnonValues, "constructorLessAccessible", vf),
										unstableAnnons.replaceAll(",", ";"),
										jarV1.toAbsolutePath().toString().replace("\\", "/"), // Replace for Windows
										jarV2.toAbsolutePath().toString().replace("\\", "/"),
										deltaPath.toAbsolutePath().toString().replace("\\", "/"),
										-1,
										t
									));
									csv.flush();
								} catch (IOException e) {
									logger.error("IOE:", e);
								}
							}
							return null;
						});

						return null;
					} else {
						synchronized (csv) {
							try {
								csv.append(String.format(csvFormat,
									group, artifact, v1, v2, level, "unknown", year, distance, ageDiff, clients, clientGroups, releases, age, activity,
									-1, -1, -1, -1, -1, -1,
									// Delta
									-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
									-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
									-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1,
									// Stable delta
									-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
									-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
									-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1,
									// Unstable delta
									-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
									-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
									-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1,
									// Unstable pkg delta
									-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
									-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
									-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1,
									// Unstable annon delta
									-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
									-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
									-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1, "{}",
									-1, -1, -1,
									"jar-not-found",
									Duration.between(t1, Instant.now()).getSeconds()
								));
							} catch (IOException e) {
								logger.error("IOE:", e);
							}
						}
					}

					return null;
				}, executor).exceptionally(t -> {
					logger.error("Delta computation failed", t);
					synchronized (csv) {
						try {
							csv.append(String.format(csvFormat,
								group, artifact, v1, v2, level, "unknown", year, distance, ageDiff, clients, clientGroups, releases, age, activity,
								-1, -1, -1, -1, -1, -1,
								// Delta
								-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
								-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
								-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1,
								// Stable delta
								-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
								-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
								-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1,
								// Unstable delta
								-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
								-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
								-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1,
								// Unstable pkg delta
								-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
								-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
								-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1,
								// Unstable annon delta
								-1, false, false, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
								-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1, -1,	-1, -1, -1,
								-1, -1, -1, -1,	-1, -1, -1, -1, -1, -1,	-1, -1, -1, -1, -1, -1, -1, "{}",
								-1, -1, -1,
								"exception-raised",
								-1
							));
						} catch (IOException e) {
							logger.error("IOE:", e);
						}
					}
					return null;
				}));
			}

			CompletableFuture<Void> f = CompletableFuture.allOf(tasks.toArray(new CompletableFuture[tasks.size()]));
			f.join();
			logger.info("All futures completed successfully. {}", f);

			executor.shutdown();
			try {
				if (!executor.awaitTermination(5, TimeUnit.SECONDS))
					executor.shutdownNow();
			} catch (InterruptedException e) {
				executor.shutdownNow();
			}
		} catch (IOException e) {
			logger.error("IOE:", e);
		}
	}

	private Map<String, Object> createMapFromIMap(IMap map) {
		Map<String, Object> values = new HashMap<>();
		map.forEach((v) -> values.put(((IString) v).getValue(), map.get(v)));
		return values;
	}

	private int getSafeInt(Map<String, Object> values, String key, IValueFactory vf) {
		return ((IInteger) values.getOrDefault(key, vf.integer(0))).intValue();
	}

	private boolean getSafeBool(Map<String, Object> values, String key, IValueFactory vf) {
		return ((IBool) values.getOrDefault(key, vf.bool(false))).getValue();
	}

	private void fetchClientsFromDeltasSample(String deltasPath, String deltasSamplePath, String clientsPath) {
		if (!Files.exists(Paths.get(deltasSamplePath)))
			sample(DELTAS_SAMPLE_SCRIPT, deltasPath, deltasSamplePath);

		if (!Files.exists(Paths.get(clientsPath))) {
			try (
				Driver driver = GraphDatabase.driver(host, AuthTokens.basic(user, password));
				Session session = driver.session(AccessMode.READ);
				CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(deltasSamplePath));
				FileWriter clientsCsv = new FileWriter(clientsPath)
			) {
				clientsCsv.write("cgroup,cartifact,cversion,cyear,group,artifact,v1,v2,level,year,jar_v1,jar_v2,delta\n");
				clientsCsv.flush();

				Map<String, String> line;
				while ((line = reader.readMap()) != null) {
					String group    = line.get("group");
					String artifact = line.get("artifact");
					String v1       = line.get("v1");
					String v2       = line.get("v2");
					String level    = line.get("level");
					int year        = Integer.parseInt(line.get("year"));
					String jarV1    = line.get("jar_v1");
					String jarV2    = line.get("jar_v2");
					String delta    = line.get("delta");

					String clientPath = CLIENTS_PATH + group + "-" + artifact + "-" + v1 + ".csv";
					File file = new File(clientPath);
					file.getParentFile().mkdirs();

					if (!file.exists()) {
						logger.info("Fetching clients of {}:{}:{}...", group, artifact, v1);

						StatementResult result = session.run(
								"MATCH (c:Artifact)-[d:DEPENDS_ON]->(a1) " +
								"WHERE a1.artifact = {a} " +
									"AND a1.groupID = {g} " +
									"AND a1.version = {v1} " +
									"AND c.groupID <> a1.groupID " +
									"AND c.version =~ {r}" +

								"RETURN DISTINCT c.groupID AS cgroup, " +
									"c.artifact AS cartifact, " +
									"c.version AS cv, " +
									"datetime(c.release_date).year AS cyear, " +
									"c.packaging AS cp, " +
									"a1.groupID AS group, " +
									"a1.artifact AS artifact, " +
									"a1.version AS v, " +
									"datetime(a1.release_date).year AS year ",
								ImmutableMap.<String, Object>builder()
									.put("a", artifact)
									.put("g", group)
									.put("v1", v1)
									.put("r", "(" + SemVerUtils.SEMVER_PATTERN + ")")
									.build()
							);

						try (FileWriter clientCsv = new FileWriter(clientPath)) {
							clientCsv.write("cgroup,cartifact,cversion,cyear,group,artifact,v1,v2,level,year,jar_v1,jar_v2,delta\n");

							while (result.hasNext()) {
								Record row = result.next();

								String cgroup     = row.get("cgroup").asString();
								String cartifact  = row.get("cartifact").asString();
								String cv         = row.get("cv").asString();
								String cp         = row.get("cp").asString();
								int cyear         = row.get("cyear").asInt();

								if (!cp.equals("Jar")) {
									continue;
								}
								else {
									clientCsv.append(String.format("%s,%s,%s,%d,%s,%s,%s,%s,%s,%d,%s,%s,%s%n",
											cgroup, cartifact, cv, cyear, group, artifact, v1, v2, level, year, jarV1, jarV2, delta));
									clientCsv.flush();
								}
							}
						}
					}

					logger.info("Sampling clients of {}:{}:{}...", group, artifact, v1);
					sample(CLIENTS_SAMPLE_SCRIPT, clientPath, clientsPath);
				}
			} catch (FileNotFoundException e) {
				logger.error("FileNotFound", e);
			} catch (IOException e) {
				logger.error("IO", e);
			}
		}
	}

	public void buildDetectionsDataset() {
		buildDetectionsDataset(DELTAS, DELTAS_SAMPLE, CLIENTS, DETECTIONS);
	}

	public void buildDetectionsRaemaekersDataset() {
		buildDetectionsDataset(DELTAS_RAEMAEKERS, DELTAS_RAEMAEKERS_SAMPLE, CLIENTS_RAEMAEKERS, DETECTIONS_RAEMAEKERS);
	}

	private void buildDetectionsDataset(String deltasPath, String deltasSamplePath, String clientsPath, String detectsPath) {
		fetchClientsFromDeltasSample(deltasPath, deltasSamplePath, clientsPath);

		File fd = new File(detectsPath);
		ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
		try (
			CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(clientsPath));
			FileWriter detectionsCsv = new FileWriter(fd, true);
		) {
			// Writing headers
			String[] suffixes = { "", "_stable", "_unstable", "_changed", "_breaking", "_nonbreaking", "_unused"};
			String[] metaArray = { "numDetections", "impactedTypes", "impactedMethods", "impactedFields" };
			String[] bcsArray = { "annotationDeprecatedAdded", "classRemoved", "classNowAbstract", "classNowFinal", "classNoLongerPublic",
					"classTypeChanged", "classNowCheckedException", "classLessAccessible", "superclassRemoved", "superclassAdded",
					"superclassModifiedIncompatible", "interfaceAdded", "interfaceRemoved", "methodRemoved", "methodRemovedInSuperclass",
					"methodLessAccessible", "methodLessAccessibleThanInSuperclass", "methodMoreAccessible", "methodIsStaticAndOverridesNotStatic",
					"methodReturnTypeChanged", "methodNowAbstract", "methodNowFinal", "methodNowStatic", "methodNoLongerStatic",
					"methodAddedToInterface", "methodAddedToPublicClass", "methodNowThrowsCheckedException", "methodAbstractAddedToClass",
					"methodAbstractAddedInSuperclass", "methodAbstractAddedInImplementedInterface", "methodNewDefault", "methodAbstractNowDefault",
					"fieldStaticAndOverridesStatic", "fieldLessAccessibleThanInSuperclass", "fieldNowFinal", "fieldNowStatic", "fieldNoLongerStatic",
					"fieldTypeChanged", "fieldRemoved", "fieldRemovedInSuperclass", "fieldLessAccessible", "fieldMoreAccessible",
					"constructorRemoved", "constructorLessAccessible" };

			if (fd.length() == 0) {
				String detectionsHeader = "cgroup,cartifact,cv,cyear,lgroup,lartifact,lv1,lv2,level,year,java_version,declarations,api_declarations,"; // Meta
				for (String suffix : suffixes) {
					if (suffix.equals("") || suffix.equals("_stable") || suffix.equals("_unstable")) {
						for (String meta : metaArray) {
							detectionsHeader += meta + suffix + ","; // Detections meta
						}
					}

					for (String bc : bcsArray) {
						detectionsHeader += bc + suffix + ","; // <Detections details>
					}
				}
				detectionsHeader += "delta,detection,exception,time\n"; // Closing data
				detectionsCsv.append(detectionsHeader);
				detectionsCsv.flush();
			}

			// Defining CSV format
			String format = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,"; // Meta
			String metaFormat = "%d,%d,%d,%d,"; // Detections meta
			String bcsFormat = "%d,%d,%d,%d,%d," // <Detections details>
					+ "%d,%d,%d,%d,"
					+ "%d,%d,%d,%d,"
					+ "%d,%d,%d,%d,%d,"
					+ "%d,%d,%d,%d,"
					+ "%d,%d,%d,%d,%d,"
					+ "%d,%d,%d,"
					+ "%d,%d,%d,%d,"
					+ "%d,%d,%d,%d,%d,%d,"
					+ "%d,%d,%d,%d,"; // </Detections details>

			for (String suffix : suffixes) {
				if (suffix.equals("") || suffix.equals("_stable") || suffix.equals("_unstable")) {
					format += metaFormat;
				}
				format += bcsFormat;
			}
			format += "%s,%s,%s,%d%n"; // Closing data
			String csvFormat = format; // Effectively final variable

			Map<String, String> line;
			Set<String> seen = new HashSet<>();
			CSVReaderHeaderAware previous = new CSVReaderHeaderAware(new FileReader(fd));
			while ((line = previous.readMap()) != null) {
				String lgroup    = line.get("lgroup");
				String lartifact = line.get("lartifact");
				String lv1       = line.get("lv1");
				String lv2       = line.get("lv2");
				String cgroup    = line.get("cgroup");
				String cartifact = line.get("cartifact");
				String cv        = line.get("cv");
				String uuid      = String.format("%s:%s:%s:%s:%s:%s:%s",
					lgroup, lartifact, lv1, lv2, cgroup, cartifact, cv);

				seen.add(uuid);
			}

			List<CompletableFuture<Object>> tasks = new ArrayList<>();
			ConcurrentSoftReferenceObjectPool<Evaluator> pool = getEvaluatorPool();
			while ((line = reader.readMap()) != null) {
				String lgroup    = line.get("group");
				String lartifact = line.get("artifact");
				String lv1       = line.get("v1");
				String lv2       = line.get("v2");
				String level     = line.get("level");
				String year      = line.get("year");
				String ljarV1    = line.get("jar_v1");
				String ljarV2    = line.get("jar_v2");
				String cgroup    = line.get("cgroup");
				String cartifact = line.get("cartifact");
				String cv        = line.get("cversion");
				String cyear     = line.get("cyear");
				String ldelta    = line.get("delta");
				String uuid      = String.format("%s:%s:%s:%s:%s:%s:%s",
						lgroup, lartifact, lv1, lv2, cgroup, cartifact, cv);

				if (seen.contains(uuid))
					continue;

				tasks.add(CompletableFuture.supplyAsync(() -> {
					Instant t1 = Instant.now();
					Artifact client = downloader.downloadArtifact(new DefaultArtifact(cgroup, cartifact, "jar", cv));

					if (client != null && client.getFile().exists()) {
						Path jar = client.getFile().toPath();
						JarStatistics jarStats = new JarStatistics();
						jarStats.compute(jar);

						boolean wrongVersion = jarStats.getJavaVersion() > 52;
						boolean wrongLanguage = !jarStats.getLanguage().equals("java");

						if (wrongVersion || wrongLanguage) {
							synchronized (detectionsCsv) {
								try {
									detectionsCsv.append(String.format(csvFormat,
										cgroup, cartifact, cv, cyear, lgroup, lartifact, lv1, lv2, level, year, jarStats.getJavaVersion(), -1, -1, // Meta
										-1, -1, -1, -1, // Detections meta
										-1, -1, -1, -1, -1, // <Detections details>
										-1, -1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1,
										-1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1, -1,
										-1, -1, -1, -1, // </Detections details>

										// Stable detects
										-1, -1, -1, -1, // Detections meta
										-1, -1, -1, -1, -1, // <Detections details>
										-1, -1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1,
										-1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1, -1,
										-1, -1, -1, -1, // </Detections details>

										// Unstable detects
										-1, -1, -1, -1, // Detections meta
										-1, -1, -1, -1, -1, // <Detections details>
										-1, -1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1,
										-1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1, -1,
										-1, -1, -1, -1, // </Detections details>

										// Changed entities
										-1, -1, -1, -1, -1, // <Detections details>
										-1, -1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1,
										-1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1, -1,
										-1, -1, -1, -1, // </Detections details>

										// Breaking entities
										-1, -1, -1, -1, -1, // <Detections details>
										-1, -1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1,
										-1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1, -1,
										-1, -1, -1, -1, // </Detections details>

										// Non-breaking entities
										-1, -1, -1, -1, -1, // <Detections details>
										-1, -1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1,
										-1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1, -1,
										-1, -1, -1, -1, // </Detections details>

										// Unused entities
										-1, -1, -1, -1, -1, // <Detections details>
										-1, -1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1,
										-1, -1, -1,
										-1, -1, -1, -1,
										-1, -1, -1, -1, -1, -1,
										-1, -1, -1, -1, // </Detections details>

										-1, -1,
										wrongVersion ? "invalid-java-version" : "invalid-language",
										Duration.between(t1, Instant.now()).getSeconds()));
									detectionsCsv.flush();
								}
								catch (IOException e) {
									logger.error("IO", e);
								}
							}

							return null; // No point losing time on the wrong versions
						}

						logger.info("Computing detections for {}:{}:{} on Δ({}:{}, {} -> {})", cgroup, cartifact, cv, lgroup, lartifact, lv1, lv2);

						pool.useAndReturn((eval) -> {
							IValueFactory vf = eval.getValueFactory();
							Prelude prelude = new Prelude(vf, new PrintWriter(System.out), new PrintWriter(System.err));

							Path jarV1 = Paths.get(ljarV1);
							Path jarV2 = Paths.get(ljarV2);
							Path deltaPath = Paths.get(ldelta);
							Path m3V1Path = Paths.get(M3S_PATH, lgroup, lartifact, String.format("%s.m3", lv1));
							Path m3V2Path = Paths.get(M3S_PATH, lgroup, lartifact, String.format("%s.m3", lv2));
							Path m3ClientPath = Paths.get(M3S_PATH, cgroup, cartifact, String.format("%s.m3", cv));

							if (!jarV1.toString().isEmpty() && !jarV2.toString().isEmpty() && !deltaPath.toString().isEmpty()) {
								IValue delta = (IValue) eval.call("readBinaryDelta", vf.sourceLocation(ldelta));
								IValue m3V1 = computeM3(eval, m3V1Path, ljarV1, true);
								IValue m3V2 = computeM3(eval, m3V2Path, ljarV2, true);
								IValue m3Client = computeM3(eval, m3ClientPath, client.getFile().getAbsolutePath(), false);
								IValue evol = (IValue) eval.call("createEvolution", m3Client, m3V1, m3V2, delta);

								Path detectionsPath = Paths.get(DETECTIONS_PATH, lgroup, lartifact, String.format("%s_to_%s.delta", lv1, lv2),
										String.format("%s_%s_%s.detect", cgroup, cartifact, cv)).toAbsolutePath();
								new File(detectionsPath.toString()).getParentFile().mkdirs();

								ISet detections;

								if (detectionsPath.toFile().exists()) {
									detections = (ISet) eval.call("readBinaryDetections", vf.sourceLocation(detectionsPath.toString()));
								}
								else {

									detections = (ISet) eval.call("computeDetections", m3Client, m3V1, m3V2, delta);
									prelude.writeBinaryValueFile(vf.sourceLocation(detectionsPath.toString()), detections, vf.bool(false));
								}

								IMap stats = (IMap) eval.call("detectionsStats", detections);
								IMap stableStats = (IMap) eval.call("stableDetectsStats", detections, delta);
								IMap unstableStats = (IMap) eval.call("unstableDetectsStats", detections, delta);
								IMap changedStats = (IMap) eval.call("changedStats", delta);
								IMap breakingStats = (IMap) eval.call("breakingStats", detections);
								IMap nonBreakingStats = (IMap) eval.call("nonBreakingStats", evol, detections);
								IMap unusedStats = (IMap) eval.call("unusedStats", evol, detections);

								Map<String, Object> values = createMapFromIMap(stats);
								Map<String, Object> stableValues = createMapFromIMap(stableStats);
								Map<String, Object> unstableValues = createMapFromIMap(unstableStats);
								Map<String, Object> changedValues = createMapFromIMap(changedStats);
								Map<String, Object> breakingValues = createMapFromIMap(breakingStats);
								Map<String, Object> nonBreakingValues = createMapFromIMap(nonBreakingStats);
								Map<String, Object> unusedValues = createMapFromIMap(unusedStats);

								int numDetects = getSafeInt(values, "numDetects", vf);
								int impactedTypes = getSafeInt(values, "impactedTypes", vf);
								int impactedMethods = getSafeInt(values, "impactedMethods", vf);
								int impactedFields = getSafeInt(values, "impactedFields", vf);

								int numDetectsStable = getSafeInt(stableValues, "numDetects", vf);
								int impactedTypesStable = getSafeInt(stableValues, "impactedTypes", vf);
								int impactedMethodsStable = getSafeInt(stableValues, "impactedMethods", vf);
								int impactedFieldsStable = getSafeInt(stableValues, "impactedFields", vf);

								int numDetectsUnstable = getSafeInt(unstableValues, "numDetects", vf);
								int impactedTypesUnstable = getSafeInt(unstableValues, "impactedTypes", vf);
								int impactedMethodsUnstable = getSafeInt(unstableValues, "impactedMethods", vf);
								int impactedFieldsUnstable = getSafeInt(unstableValues, "impactedFields", vf);

								long t = Duration.between(t1, Instant.now()).getSeconds();
								logger.info("Computing detections for {}:{}:{} on Δ({}, {}, {} -> {}) took {}s",
									cgroup, cartifact, cv, lgroup, lartifact, lv1, lv2, t);

								synchronized (detectionsCsv) {
									try {
										detectionsCsv.append(String.format(csvFormat,
											cgroup, cartifact, cv, cyear, lgroup, lartifact, lv1, lv2, level, year,
											jarStats.getJavaVersion(), jarStats.getDeclarations(),jarStats.getAPIDeclarations(),
											numDetects, impactedTypes, impactedMethods, impactedFields,
											getSafeInt(values, "annotationDeprecatedAdded", vf),
											getSafeInt(values, "classRemoved", vf),
											getSafeInt(values, "classNowAbstract", vf),
											getSafeInt(values, "classNowFinal", vf),
											getSafeInt(values, "classNoLongerPublic", vf),
											getSafeInt(values, "classTypeChanged", vf),
											getSafeInt(values, "classNowCheckedException", vf),
											getSafeInt(values, "classLessAccessible", vf),
											getSafeInt(values, "superclassRemoved", vf),
											getSafeInt(values, "superclassAdded", vf),
											getSafeInt(values, "superclassModifiedIncompatible", vf),
											getSafeInt(values, "interfaceAdded", vf),
											getSafeInt(values, "interfaceRemoved", vf),
											getSafeInt(values, "methodRemoved", vf),
											getSafeInt(values, "methodRemovedInSuperclass", vf),
											getSafeInt(values, "methodLessAccessible", vf),
											getSafeInt(values, "methodLessAccessibleThanInSuperclass", vf),
											getSafeInt(values, "methodMoreAccessible", vf),
											getSafeInt(values, "methodIsStaticAndOverridesNotStatic", vf),
											getSafeInt(values, "methodReturnTypeChanged", vf),
											getSafeInt(values, "methodNowAbstract", vf),
											getSafeInt(values, "methodNowFinal", vf),
											getSafeInt(values, "methodNowStatic", vf),
											getSafeInt(values, "methodNoLongerStatic", vf),
											getSafeInt(values, "methodAddedToInterface", vf),
											getSafeInt(values, "methodAddedToPublicClass", vf),
											getSafeInt(values, "methodNowThrowsCheckedException", vf),
											getSafeInt(values, "methodAbstractAddedToClass", vf),
											getSafeInt(values, "methodAbstractAddedInSuperclass", vf),
											getSafeInt(values, "methodAbstractAddedInImplementedInterface", vf),
											getSafeInt(values, "methodNewDefault", vf),
											getSafeInt(values, "methodAbstractNowDefault", vf),
											getSafeInt(values, "fieldStaticAndOverridesStatic", vf),
											getSafeInt(values, "fieldLessAccessibleThanInSuperclass", vf),
											getSafeInt(values, "fieldNowFinal", vf),
											getSafeInt(values, "fieldNowStatic", vf),
											getSafeInt(values, "fieldNoLongerStatic", vf),
											getSafeInt(values, "fieldTypeChanged", vf),
											getSafeInt(values, "fieldRemoved", vf),
											getSafeInt(values, "fieldRemovedInSuperclass", vf),
											getSafeInt(values, "fieldLessAccessible", vf),
											getSafeInt(values, "fieldMoreAccessible", vf),
											getSafeInt(values, "constructorRemoved", vf),
											getSafeInt(values, "constructorLessAccessible", vf),

											// Stable detects
											numDetectsStable, impactedTypesStable, impactedMethodsStable, impactedFieldsStable,
											getSafeInt(stableValues, "annotationDeprecatedAdded", vf),
											getSafeInt(stableValues, "classRemoved", vf),
											getSafeInt(stableValues, "classNowAbstract", vf),
											getSafeInt(stableValues, "classNowFinal", vf),
											getSafeInt(stableValues, "classNoLongerPublic", vf),
											getSafeInt(stableValues, "classTypeChanged", vf),
											getSafeInt(stableValues, "classNowCheckedException", vf),
											getSafeInt(stableValues, "classLessAccessible", vf),
											getSafeInt(stableValues, "superclassRemoved", vf),
											getSafeInt(stableValues, "superclassAdded", vf),
											getSafeInt(stableValues, "superclassModifiedIncompatible", vf),
											getSafeInt(stableValues, "interfaceAdded", vf),
											getSafeInt(stableValues, "interfaceRemoved", vf),
											getSafeInt(stableValues, "methodRemoved", vf),
											getSafeInt(stableValues, "methodRemovedInSuperclass", vf),
											getSafeInt(stableValues, "methodLessAccessible", vf),
											getSafeInt(stableValues, "methodLessAccessibleThanInSuperclass", vf),
											getSafeInt(stableValues, "methodMoreAccessible", vf),
											getSafeInt(stableValues, "methodIsStaticAndOverridesNotStatic", vf),
											getSafeInt(stableValues, "methodReturnTypeChanged", vf),
											getSafeInt(stableValues, "methodNowAbstract", vf),
											getSafeInt(stableValues, "methodNowFinal", vf),
											getSafeInt(stableValues, "methodNowStatic", vf),
											getSafeInt(stableValues, "methodNoLongerStatic", vf),
											getSafeInt(stableValues, "methodAddedToInterface", vf),
											getSafeInt(stableValues, "methodAddedToPublicClass", vf),
											getSafeInt(stableValues, "methodNowThrowsCheckedException", vf),
											getSafeInt(stableValues, "methodAbstractAddedToClass", vf),
											getSafeInt(stableValues, "methodAbstractAddedInSuperclass", vf),
											getSafeInt(stableValues, "methodAbstractAddedInImplementedInterface", vf),
											getSafeInt(stableValues, "methodNewDefault", vf),
											getSafeInt(stableValues, "methodAbstractNowDefault", vf),
											getSafeInt(stableValues, "fieldStaticAndOverridesStatic", vf),
											getSafeInt(stableValues, "fieldLessAccessibleThanInSuperclass", vf),
											getSafeInt(stableValues, "fieldNowFinal", vf),
											getSafeInt(stableValues, "fieldNowStatic", vf),
											getSafeInt(stableValues, "fieldNoLongerStatic", vf),
											getSafeInt(stableValues, "fieldTypeChanged", vf),
											getSafeInt(stableValues, "fieldRemoved", vf),
											getSafeInt(stableValues, "fieldRemovedInSuperclass", vf),
											getSafeInt(stableValues, "fieldLessAccessible", vf),
											getSafeInt(stableValues, "fieldMoreAccessible", vf),
											getSafeInt(stableValues, "constructorRemoved", vf),
											getSafeInt(stableValues, "constructorLessAccessible", vf),

											// Unstable detects
											numDetectsUnstable, impactedTypesUnstable, impactedMethodsUnstable, impactedFieldsUnstable,
											getSafeInt(unstableValues, "annotationDeprecatedAdded", vf),
											getSafeInt(unstableValues, "classRemoved", vf),
											getSafeInt(unstableValues, "classNowAbstract", vf),
											getSafeInt(unstableValues, "classNowFinal", vf),
											getSafeInt(unstableValues, "classNoLongerPublic", vf),
											getSafeInt(unstableValues, "classTypeChanged", vf),
											getSafeInt(unstableValues, "classNowCheckedException", vf),
											getSafeInt(unstableValues, "classLessAccessible", vf),
											getSafeInt(unstableValues, "superclassRemoved", vf),
											getSafeInt(unstableValues, "superclassAdded", vf),
											getSafeInt(unstableValues, "superclassModifiedIncompatible", vf),
											getSafeInt(unstableValues, "interfaceAdded", vf),
											getSafeInt(unstableValues, "interfaceRemoved", vf),
											getSafeInt(unstableValues, "methodRemoved", vf),
											getSafeInt(unstableValues, "methodRemovedInSuperclass", vf),
											getSafeInt(unstableValues, "methodLessAccessible", vf),
											getSafeInt(unstableValues, "methodLessAccessibleThanInSuperclass", vf),
											getSafeInt(unstableValues, "methodMoreAccessible", vf),
											getSafeInt(unstableValues, "methodIsStaticAndOverridesNotStatic", vf),
											getSafeInt(unstableValues, "methodReturnTypeChanged", vf),
											getSafeInt(unstableValues, "methodNowAbstract", vf),
											getSafeInt(unstableValues, "methodNowFinal", vf),
											getSafeInt(unstableValues, "methodNowStatic", vf),
											getSafeInt(unstableValues, "methodNoLongerStatic", vf),
											getSafeInt(unstableValues, "methodAddedToInterface", vf),
											getSafeInt(unstableValues, "methodAddedToPublicClass", vf),
											getSafeInt(unstableValues, "methodNowThrowsCheckedException", vf),
											getSafeInt(unstableValues, "methodAbstractAddedToClass", vf),
											getSafeInt(unstableValues, "methodAbstractAddedInSuperclass", vf),
											getSafeInt(unstableValues, "methodAbstractAddedInImplementedInterface", vf),
											getSafeInt(unstableValues, "methodNewDefault", vf),
											getSafeInt(unstableValues, "methodAbstractNowDefault", vf),
											getSafeInt(unstableValues, "fieldStaticAndOverridesStatic", vf),
											getSafeInt(unstableValues, "fieldLessAccessibleThanInSuperclass", vf),
											getSafeInt(unstableValues, "fieldNowFinal", vf),
											getSafeInt(unstableValues, "fieldNowStatic", vf),
											getSafeInt(unstableValues, "fieldNoLongerStatic", vf),
											getSafeInt(unstableValues, "fieldTypeChanged", vf),
											getSafeInt(unstableValues, "fieldRemoved", vf),
											getSafeInt(unstableValues, "fieldRemovedInSuperclass", vf),
											getSafeInt(unstableValues, "fieldLessAccessible", vf),
											getSafeInt(unstableValues, "fieldMoreAccessible", vf),
											getSafeInt(unstableValues, "constructorRemoved", vf),
											getSafeInt(unstableValues, "constructorLessAccessible", vf),

											// Changed entities
											getSafeInt(changedValues, "annotationDeprecatedAdded", vf),
											getSafeInt(changedValues, "classRemoved", vf),
											getSafeInt(changedValues, "classNowAbstract", vf),
											getSafeInt(changedValues, "classNowFinal", vf),
											getSafeInt(changedValues, "classNoLongerPublic", vf),
											getSafeInt(changedValues, "classTypeChanged", vf),
											getSafeInt(changedValues, "classNowCheckedException", vf),
											getSafeInt(changedValues, "classLessAccessible", vf),
											getSafeInt(changedValues, "superclassRemoved", vf),
											getSafeInt(changedValues, "superclassAdded", vf),
											getSafeInt(changedValues, "superclassModifiedIncompatible", vf),
											getSafeInt(changedValues, "interfaceAdded", vf),
											getSafeInt(changedValues, "interfaceRemoved", vf),
											getSafeInt(changedValues, "methodRemoved", vf),
											getSafeInt(changedValues, "methodRemovedInSuperclass", vf),
											getSafeInt(changedValues, "methodLessAccessible", vf),
											getSafeInt(changedValues, "methodLessAccessibleThanInSuperclass", vf),
											getSafeInt(changedValues, "methodMoreAccessible", vf),
											getSafeInt(changedValues, "methodIsStaticAndOverridesNotStatic", vf),
											getSafeInt(changedValues, "methodReturnTypeChanged", vf),
											getSafeInt(changedValues, "methodNowAbstract", vf),
											getSafeInt(changedValues, "methodNowFinal", vf),
											getSafeInt(changedValues, "methodNowStatic", vf),
											getSafeInt(changedValues, "methodNoLongerStatic", vf),
											getSafeInt(changedValues, "methodAddedToInterface", vf),
											getSafeInt(changedValues, "methodAddedToPublicClass", vf),
											getSafeInt(changedValues, "methodNowThrowsCheckedException", vf),
											getSafeInt(changedValues, "methodAbstractAddedToClass", vf),
											getSafeInt(changedValues, "methodAbstractAddedInSuperclass", vf),
											getSafeInt(changedValues, "methodAbstractAddedInImplementedInterface", vf),
											getSafeInt(changedValues, "methodNewDefault", vf),
											getSafeInt(changedValues, "methodAbstractNowDefault", vf),
											getSafeInt(changedValues, "fieldStaticAndOverridesStatic", vf),
											getSafeInt(changedValues, "fieldLessAccessibleThanInSuperclass", vf),
											getSafeInt(changedValues, "fieldNowFinal", vf),
											getSafeInt(changedValues, "fieldNowStatic", vf),
											getSafeInt(changedValues, "fieldNoLongerStatic", vf),
											getSafeInt(changedValues, "fieldTypeChanged", vf),
											getSafeInt(changedValues, "fieldRemoved", vf),
											getSafeInt(changedValues, "fieldRemovedInSuperclass", vf),
											getSafeInt(changedValues, "fieldLessAccessible", vf),
											getSafeInt(changedValues, "fieldMoreAccessible", vf),
											getSafeInt(changedValues, "constructorRemoved", vf),
											getSafeInt(changedValues, "constructorLessAccessible", vf),

											// Breaking entities
											getSafeInt(breakingValues, "annotationDeprecatedAdded", vf),
											getSafeInt(breakingValues, "classRemoved", vf),
											getSafeInt(breakingValues, "classNowAbstract", vf),
											getSafeInt(breakingValues, "classNowFinal", vf),
											getSafeInt(breakingValues, "classNoLongerPublic", vf),
											getSafeInt(breakingValues, "classTypeChanged", vf),
											getSafeInt(breakingValues, "classNowCheckedException", vf),
											getSafeInt(breakingValues, "classLessAccessible", vf),
											getSafeInt(breakingValues, "superclassRemoved", vf),
											getSafeInt(breakingValues, "superclassAdded", vf),
											getSafeInt(breakingValues, "superclassModifiedIncompatible", vf),
											getSafeInt(breakingValues, "interfaceAdded", vf),
											getSafeInt(breakingValues, "interfaceRemoved", vf),
											getSafeInt(breakingValues, "methodRemoved", vf),
											getSafeInt(breakingValues, "methodRemovedInSuperclass", vf),
											getSafeInt(breakingValues, "methodLessAccessible", vf),
											getSafeInt(breakingValues, "methodLessAccessibleThanInSuperclass", vf),
											getSafeInt(breakingValues, "methodMoreAccessible", vf),
											getSafeInt(breakingValues, "methodIsStaticAndOverridesNotStatic", vf),
											getSafeInt(breakingValues, "methodReturnTypeChanged", vf),
											getSafeInt(breakingValues, "methodNowAbstract", vf),
											getSafeInt(breakingValues, "methodNowFinal", vf),
											getSafeInt(breakingValues, "methodNowStatic", vf),
											getSafeInt(breakingValues, "methodNoLongerStatic", vf),
											getSafeInt(breakingValues, "methodAddedToInterface", vf),
											getSafeInt(breakingValues, "methodAddedToPublicClass", vf),
											getSafeInt(breakingValues, "methodNowThrowsCheckedException", vf),
											getSafeInt(breakingValues, "methodAbstractAddedToClass", vf),
											getSafeInt(breakingValues, "methodAbstractAddedInSuperclass", vf),
											getSafeInt(breakingValues, "methodAbstractAddedInImplementedInterface", vf),
											getSafeInt(breakingValues, "methodNewDefault", vf),
											getSafeInt(breakingValues, "methodAbstractNowDefault", vf),
											getSafeInt(breakingValues, "fieldStaticAndOverridesStatic", vf),
											getSafeInt(breakingValues, "fieldLessAccessibleThanInSuperclass", vf),
											getSafeInt(breakingValues, "fieldNowFinal", vf),
											getSafeInt(breakingValues, "fieldNowStatic", vf),
											getSafeInt(breakingValues, "fieldNoLongerStatic", vf),
											getSafeInt(breakingValues, "fieldTypeChanged", vf),
											getSafeInt(breakingValues, "fieldRemoved", vf),
											getSafeInt(breakingValues, "fieldRemovedInSuperclass", vf),
											getSafeInt(breakingValues, "fieldLessAccessible", vf),
											getSafeInt(breakingValues, "fieldMoreAccessible", vf),
											getSafeInt(breakingValues, "constructorRemoved", vf),
											getSafeInt(breakingValues, "constructorLessAccessible", vf),

											// Non-breaking entities
											getSafeInt(nonBreakingValues, "annotationDeprecatedAdded", vf),
											getSafeInt(nonBreakingValues, "classRemoved", vf),
											getSafeInt(nonBreakingValues, "classNowAbstract", vf),
											getSafeInt(nonBreakingValues, "classNowFinal", vf),
											getSafeInt(nonBreakingValues, "classNoLongerPublic", vf),
											getSafeInt(nonBreakingValues, "classTypeChanged", vf),
											getSafeInt(nonBreakingValues, "classNowCheckedException", vf),
											getSafeInt(nonBreakingValues, "classLessAccessible", vf),
											getSafeInt(nonBreakingValues, "superclassRemoved", vf),
											getSafeInt(nonBreakingValues, "superclassAdded", vf),
											getSafeInt(nonBreakingValues, "superclassModifiedIncompatible", vf),
											getSafeInt(nonBreakingValues, "interfaceAdded", vf),
											getSafeInt(nonBreakingValues, "interfaceRemoved", vf),
											getSafeInt(nonBreakingValues, "methodRemoved", vf),
											getSafeInt(nonBreakingValues, "methodRemovedInSuperclass", vf),
											getSafeInt(nonBreakingValues, "methodLessAccessible", vf),
											getSafeInt(nonBreakingValues, "methodLessAccessibleThanInSuperclass", vf),
											getSafeInt(nonBreakingValues, "methodMoreAccessible", vf),
											getSafeInt(nonBreakingValues, "methodIsStaticAndOverridesNotStatic", vf),
											getSafeInt(nonBreakingValues, "methodReturnTypeChanged", vf),
											getSafeInt(nonBreakingValues, "methodNowAbstract", vf),
											getSafeInt(nonBreakingValues, "methodNowFinal", vf),
											getSafeInt(nonBreakingValues, "methodNowStatic", vf),
											getSafeInt(nonBreakingValues, "methodNoLongerStatic", vf),
											getSafeInt(nonBreakingValues, "methodAddedToInterface", vf),
											getSafeInt(nonBreakingValues, "methodAddedToPublicClass", vf),
											getSafeInt(nonBreakingValues, "methodNowThrowsCheckedException", vf),
											getSafeInt(nonBreakingValues, "methodAbstractAddedToClass", vf),
											getSafeInt(nonBreakingValues, "methodAbstractAddedInSuperclass", vf),
											getSafeInt(nonBreakingValues, "methodAbstractAddedInImplementedInterface", vf),
											getSafeInt(nonBreakingValues, "methodNewDefault", vf),
											getSafeInt(nonBreakingValues, "methodAbstractNowDefault", vf),
											getSafeInt(nonBreakingValues, "fieldStaticAndOverridesStatic", vf),
											getSafeInt(nonBreakingValues, "fieldLessAccessibleThanInSuperclass", vf),
											getSafeInt(nonBreakingValues, "fieldNowFinal", vf),
											getSafeInt(nonBreakingValues, "fieldNowStatic", vf),
											getSafeInt(nonBreakingValues, "fieldNoLongerStatic", vf),
											getSafeInt(nonBreakingValues, "fieldTypeChanged", vf),
											getSafeInt(nonBreakingValues, "fieldRemoved", vf),
											getSafeInt(nonBreakingValues, "fieldRemovedInSuperclass", vf),
											getSafeInt(nonBreakingValues, "fieldLessAccessible", vf),
											getSafeInt(nonBreakingValues, "fieldMoreAccessible", vf),
											getSafeInt(nonBreakingValues, "constructorRemoved", vf),
											getSafeInt(nonBreakingValues, "constructorLessAccessible", vf),

											// Unused entities
											getSafeInt(unusedValues, "annotationDeprecatedAdded", vf),
											getSafeInt(unusedValues, "classRemoved", vf),
											getSafeInt(unusedValues, "classNowAbstract", vf),
											getSafeInt(unusedValues, "classNowFinal", vf),
											getSafeInt(unusedValues, "classNoLongerPublic", vf),
											getSafeInt(unusedValues, "classTypeChanged", vf),
											getSafeInt(unusedValues, "classNowCheckedException", vf),
											getSafeInt(unusedValues, "classLessAccessible", vf),
											getSafeInt(unusedValues, "superclassRemoved", vf),
											getSafeInt(unusedValues, "superclassAdded", vf),
											getSafeInt(unusedValues, "superclassModifiedIncompatible", vf),
											getSafeInt(unusedValues, "interfaceAdded", vf),
											getSafeInt(unusedValues, "interfaceRemoved", vf),
											getSafeInt(unusedValues, "methodRemoved", vf),
											getSafeInt(unusedValues, "methodRemovedInSuperclass", vf),
											getSafeInt(unusedValues, "methodLessAccessible", vf),
											getSafeInt(unusedValues, "methodLessAccessibleThanInSuperclass", vf),
											getSafeInt(unusedValues, "methodMoreAccessible", vf),
											getSafeInt(unusedValues, "methodIsStaticAndOverridesNotStatic", vf),
											getSafeInt(unusedValues, "methodReturnTypeChanged", vf),
											getSafeInt(unusedValues, "methodNowAbstract", vf),
											getSafeInt(unusedValues, "methodNowFinal", vf),
											getSafeInt(unusedValues, "methodNowStatic", vf),
											getSafeInt(unusedValues, "methodNoLongerStatic", vf),
											getSafeInt(unusedValues, "methodAddedToInterface", vf),
											getSafeInt(unusedValues, "methodAddedToPublicClass", vf),
											getSafeInt(unusedValues, "methodNowThrowsCheckedException", vf),
											getSafeInt(unusedValues, "methodAbstractAddedToClass", vf),
											getSafeInt(unusedValues, "methodAbstractAddedInSuperclass", vf),
											getSafeInt(unusedValues, "methodAbstractAddedInImplementedInterface", vf),
											getSafeInt(unusedValues, "methodNewDefault", vf),
											getSafeInt(unusedValues, "methodAbstractNowDefault", vf),
											getSafeInt(unusedValues, "fieldStaticAndOverridesStatic", vf),
											getSafeInt(unusedValues, "fieldLessAccessibleThanInSuperclass", vf),
											getSafeInt(unusedValues, "fieldNowFinal", vf),
											getSafeInt(unusedValues, "fieldNowStatic", vf),
											getSafeInt(unusedValues, "fieldNoLongerStatic", vf),
											getSafeInt(unusedValues, "fieldTypeChanged", vf),
											getSafeInt(unusedValues, "fieldRemoved", vf),
											getSafeInt(unusedValues, "fieldRemovedInSuperclass", vf),
											getSafeInt(unusedValues, "fieldLessAccessible", vf),
											getSafeInt(unusedValues, "fieldMoreAccessible", vf),
											getSafeInt(unusedValues, "constructorRemoved", vf),
											getSafeInt(unusedValues, "constructorLessAccessible", vf),
											ldelta,
											detectionsPath.toAbsolutePath().toString().replace("\\", "/"), // Replace for Windows
											-1,
											t
										));
										detectionsCsv.flush();
									}
									catch (IOException e) {
										logger.error("IOE:", e);
									}
								}
							}
							else {
								logger.info("No info for {}:{} ({},{})", lgroup, lartifact, lv1, lv2);
							}
							return null;
						});
					} else {
						synchronized (detectionsCsv) {
							try {
								detectionsCsv.append(String.format(csvFormat,
									cgroup, cartifact, cv, cyear, lgroup, lartifact, lv1, lv2, level, year, -1, -1, -1, // Meta
									-1, -1, -1, -1, // Detections meta
									-1, -1, -1, -1, -1, // <Detections details>
									-1, -1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1,
									-1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1, -1,
									-1, -1, -1, -1, // </Detections details>
									// Stable detects
									-1, -1, -1, -1, // Detections meta
									-1, -1, -1, -1, -1, // <Detections details>
									-1, -1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1,
									-1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1, -1,
									-1, -1, -1, -1, // </Detections details>
									// Unstable detects
									-1, -1, -1, -1, // Detections meta
									-1, -1, -1, -1, -1, // <Detections details>
									-1, -1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1,
									-1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1, -1,
									-1, -1, -1, -1, // </Detections details>
									// Changed entities
									-1, -1, -1, -1, -1, // <Detections details>
									-1, -1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1,
									-1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1, -1,
									-1, -1, -1, -1, // </Detections details>
									// Breaking entities
									-1, -1, -1, -1, -1, // <Detections details>
									-1, -1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1,
									-1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1, -1,
									-1, -1, -1, -1, // </Detections details>
									// Non-breaking entities
									-1, -1, -1, -1, -1, // <Detections details>
									-1, -1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1,
									-1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1, -1,
									-1, -1, -1, -1, // </Detections details>
									// Unused entities
									-1, -1, -1, -1, -1, // <Detections details>
									-1, -1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1,
									-1, -1, -1,
									-1, -1, -1, -1,
									-1, -1, -1, -1, -1, -1,
									-1, -1, -1, -1, // </Detections details>
									-1, -1,
									"jar-not-found",
									Duration.between(t1, Instant.now()).getSeconds()));
								detectionsCsv.flush();
							}
							catch (IOException e) {
								logger.error("IO", e);
							}
						}
					}

					return null;
				}, executor).exceptionally(t -> {
					logger.error("Detection computation failed", t);
					synchronized (detectionsCsv) {
						try {
							detectionsCsv.append(String.format(csvFormat,
								cgroup, cartifact, cv, cyear, lgroup, lartifact, lv1, lv2, level, year, -1, -1, -1, // Meta
								-1, -1, -1, -1, // Detections meta
								-1, -1, -1, -1, -1, // <Detections details>
								-1, -1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1,
								-1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1, -1,
								-1, -1, -1, -1, // </Detections details>
								// Stable detects
								-1, -1, -1, -1, // Detections meta
								-1, -1, -1, -1, -1, // <Detections details>
								-1, -1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1,
								-1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1, -1,
								-1, -1, -1, -1, // </Detections details>
								// Unstable detects
								-1, -1, -1, -1, // Detections meta
								-1, -1, -1, -1, -1, // <Detections details>
								-1, -1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1,
								-1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1, -1,
								-1, -1, -1, -1, // </Detections details>
								// Changed entities
								-1, -1, -1, -1, -1, // <Detections details>
								-1, -1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1,
								-1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1, -1,
								-1, -1, -1, -1, // </Detections details>
								// Breaking entities
								-1, -1, -1, -1, -1, // <Detections details>
								-1, -1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1,
								-1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1, -1,
								-1, -1, -1, -1, // </Detections details>
								// Non-breaking entities
								-1, -1, -1, -1, -1, // <Detections details>
								-1, -1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1,
								-1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1, -1,
								-1, -1, -1, -1, // </Detections details>
								// Unused entities
								-1, -1, -1, -1, -1, // <Detections details>
								-1, -1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1,
								-1, -1, -1,
								-1, -1, -1, -1,
								-1, -1, -1, -1, -1, -1,
								-1, -1, -1, -1, // </Detections details>
								-1, -1,
								"exception-raised",
								-1));
							detectionsCsv.flush();
						} catch (IOException e) {
							logger.error("IOE:", e);
						}
					}
					return null;
				}));
			}

			CompletableFuture<Void> f = CompletableFuture.allOf(tasks.toArray(new CompletableFuture[tasks.size()]));
			f.join();
			logger.info("All futures completed successfully. {}", f);

			executor.shutdown();
			try {
				if (!executor.awaitTermination(5, TimeUnit.SECONDS))
					executor.shutdownNow();
			} catch (InterruptedException e) {
				executor.shutdownNow();
			}
		}
		catch (IOException e) {
			logger.error("IOE:", e);
		}
	}

	private IValue computeM3(Evaluator eval, Path m3Path, String jarPath, boolean cache) {
		if (m3s.containsKey(m3Path)) {
			return m3s.get(m3Path);
		} else {
			IValueFactory vf = eval.getValueFactory();
			Prelude prelude = new Prelude(vf, new PrintWriter(System.out), new PrintWriter(System.err));
			IValue m3Val;

			// Create M3 parent directories
			m3Path.toFile().getParentFile().mkdirs();

			if (m3Path.toFile().exists()) {
				m3Val = (IValue) eval.call("readBinaryM3", vf.sourceLocation(m3Path.toAbsolutePath().toString()));
			}
			else {
				m3Val = (IValue) eval.call("createM3", vf.sourceLocation(jarPath));
				prelude.writeBinaryValueFile(vf.sourceLocation(m3Path.toAbsolutePath().toString()), m3Val, vf.bool(false));
			}

			if (cache)
				m3s.put(m3Path, m3Val);

			return m3Val;
		}
	}

	private boolean sample(String script, String input, String output) {
		boolean sampled = false;
		RScriptRunner runner = new RScriptRunner(logger);
		try {
			runner.runSampling(script, input, output);
			sampled = true;
		}
		catch (InterruptedException e) {
			logger.error("Interrupted", e);
		}
		catch (IOException e) {
			logger.error("IO", e);
		}
		return sampled;
	}

	private synchronized Evaluator createRascalEvaluator() {
		IValueFactory vf = ValueFactoryFactory.getValueFactory();
		GlobalEnvironment heap = new GlobalEnvironment();
		ModuleEnvironment module = new ModuleEnvironment("$maven$", heap);
		Evaluator eval = new Evaluator(vf, System.in, System.err, System.out, module, heap);

		eval.addRascalSearchPathContributor(StandardLibraryContributor.getInstance());
		ISourceLocation path = URIUtil.correctLocation("lib", "maracas", "");
		eval.addRascalSearchPath(path);

		eval.doImport(new NullRascalMonitor(), "org::maracas::delta::JApiCmp");
		eval.doImport(new NullRascalMonitor(), "org::maracas::delta::JApiCmpDetector");
		eval.doImport(new NullRascalMonitor(), "org::maracas::measure::delta::Evolution");
		eval.doImport(new NullRascalMonitor(), "org::maracas::measure::delta::Impact");
		eval.doImport(new NullRascalMonitor(), "org::maracas::m3::Core");
		eval.doImport(new NullRascalMonitor(), "lang::java::m3::Core");

		return eval;
	}

	private ConcurrentSoftReferenceObjectPool<Evaluator> getEvaluatorPool() {
		return new ConcurrentSoftReferenceObjectPool<>(5, TimeUnit.MINUTES, Runtime.getRuntime().availableProcessors(), Integer.MAX_VALUE,
			() -> { return createRascalEvaluator(); });
	}

	public static void main(String[] args) {
		OptionGroup step = new OptionGroup()
			.addOption(Option.builder("all")
				.desc("Build all datasets").build())
			.addOption(Option.builder("raemaekers")
				.desc("Build Raemaekers' datasets").build())
			.addOption(Option.builder("mdg")
				.desc("Build MDG's datasets").build())
			.addOption(Option.builder("versions")
				.desc("Build the versions.csv dataset").build())
			.addOption(Option.builder("versionsRaemaekers")
				.desc("Build the versions-raemaekers.csv dataset").build())
			.addOption(Option.builder("deltas")
				.desc("Build the deltas.csv dataset").build())
			.addOption(Option.builder("deltasRaemaekers")
				.desc("Build the deltas-raemaekers.csv dataset").build())
			.addOption(Option.builder("upgrades")
				.desc("Builds the upgrades.csv dataset").build())
			.addOption(Option.builder("upgradesRaemaekers")
				.desc("Builds the upgrades-raemaekers.csv dataset").build())
			.addOption(Option.builder("detections")
				.desc("Build the detections.csv dataset").build())
			.addOption(Option.builder("detectionsRaemaekers")
				.desc("Build the detections-raemaekers.csv dataset").build())
			.addOption(Option.builder("cleanDB")
				.desc("Remove all non-(test|compile) dependencies").build());
		step.setRequired(true);

		HelpFormatter formatter = new HelpFormatter();
		Options opts = new Options().addOptionGroup(step);

		try (FileInputStream fis = new FileInputStream("config.properties")) {
			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(opts, args);
			Properties props = new Properties();
			props.load(fis);

			String host    = props.getProperty("neo4j_host");
			String user    = props.getProperty("neo4j_user");
			String pwd     = props.getProperty("neo4j_pwd");
			int threads    = Integer.parseInt(props.getProperty("max_threads"));
			int qps        = Integer.parseInt(props.getProperty("aether_qps"));
			int cGroups    = Integer.parseInt(props.getProperty("client_groups_per_library"));
			float cochranE = Float.parseFloat(props.getProperty("cochran_e"));
			float cochranP = Float.parseFloat(props.getProperty("cochran_p"));
			BuildDataset rq = new BuildDataset(host, user, pwd, threads, qps);

			boolean all = cmd.hasOption("all");
			boolean raemaekers = cmd.hasOption("raemaekers");
			boolean mdg = cmd.hasOption("mdg");

			if (all || cmd.hasOption("cleanDB")) {
				Instant t1 = Instant.now();
				rq.cleanDB();
				Instant t2 = Instant.now();
				logger.info("cleanDB() took {} minutes.", Duration.between(t1, t2).toMinutes());
			}

			if (all || mdg || cmd.hasOption("versions")) {
				Instant t1 = Instant.now();
				rq.buildVersionsDataset(cGroups);
				Instant t2 = Instant.now();
				logger.info("buildVersionsDataset() took {} minutes.", Duration.between(t1, t2).toMinutes());
			}

			if (all || raemaekers || cmd.hasOption("versionsRaemaekers")) {
				Instant t1 = Instant.now();
				rq.buildVersionsRaemaekersDataset(cGroups);
				Instant t2 = Instant.now();
				logger.info("buildVersionsRaemaekersDataset() took {} minutes.", Duration.between(t1, t2).toMinutes());
			}

			if (all || mdg || cmd.hasOption("deltas")) {
				Instant t1 = Instant.now();
				rq.buildDeltasDataset();
				Instant t2 = Instant.now();
				logger.info("buildDeltasDataset() took {} minutes.", Duration.between(t1, t2).toMinutes());
			}

			if (all || raemaekers || cmd.hasOption("deltasRaemaekers")) {
				Instant t1 = Instant.now();
				rq.buildDeltasRaemaekersDataset();
				Instant t2 = Instant.now();
				logger.info("buildDeltasRaemaekersDataset() took {} minutes.", Duration.between(t1, t2).toMinutes());
			}

			if (all || mdg || cmd.hasOption("upgrades")) {
				Instant t1 = Instant.now();
				rq.buildUpgradesDataset();
				rq.buildUpgradesDetectionsDatasets(cochranE, cochranP);
				Instant t2 = Instant.now();
				logger.info("buildUpgradesDataset() took {} minutes.", Duration.between(t1, t2).toMinutes());
			}

			if (all || raemaekers || cmd.hasOption("upgradesRaemaekers")) {
				Instant t1 = Instant.now();
				rq.buildUpgradesRaemaekersDataset();
				rq.buildUpgradesDetectionsRaemaekersDatasets(cochranE, cochranP);
				Instant t2 = Instant.now();
				logger.info("buildUpgradesDataset() took {} minutes.", Duration.between(t1, t2).toMinutes());
			}

			if (all || mdg || cmd.hasOption("detections")) {
				Instant t1 = Instant.now();
				rq.buildDetectionsDataset();
				Instant t2 = Instant.now();
				logger.info("buildDetectionsDataset() took {} minutes.", Duration.between(t1, t2).toMinutes());
			}

			if (all || raemaekers || cmd.hasOption("detectionsRaemaekers")) {
				Instant t1 = Instant.now();
				rq.buildDetectionsRaemaekersDataset();
				Instant t2 = Instant.now();
				logger.info("buildDetectionsRaemaekersDataset() took {} minutes.", Duration.between(t1, t2).toMinutes());
			}
		} catch (ParseException e) {
			formatter.printHelp("buildDataset", opts);
		} catch (IOException e) {
			logger.error("IOE:", e);
		}
	}
}
