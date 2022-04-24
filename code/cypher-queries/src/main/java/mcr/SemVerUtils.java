package mcr;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.maven.artifact.versioning.ArtifactVersion;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;

public class SemVerUtils {
	public static final String SEMVER_PATTERN = "(\\d+)\\.(\\d+)(\\.(\\d+))?"; // X.Y[.Z]

	public enum SemLevel {
		MAJOR, MINOR, PATCH, UNKNOWN
	}

	public static class SemInfo {
		final SemLevel level;
		final int distance;
		public SemInfo(SemLevel l, int d) {
			level = l;
			distance = d;
		}
	}

	public static SemInfo computeSemInfo(String v1, String v2) {
		Pattern semVer = Pattern.compile(SEMVER_PATTERN);
		Matcher v1Matcher = semVer.matcher(v1);
		Matcher v2Matcher = semVer.matcher(v2);

		if (v1Matcher.matches() && v2Matcher.matches()) {
			double v1Major = Double.parseDouble(v1Matcher.group(1));
			double v2Major = Double.parseDouble(v2Matcher.group(1));
			double v1Minor = Double.parseDouble(v1Matcher.group(2));
			double v2Minor = Double.parseDouble(v2Matcher.group(2));
			double v1Patch = v1Matcher.group(3) != null ? Double.parseDouble(v1Matcher.group(4)) : -1;
			double v2Patch = v2Matcher.group(3) != null ? Double.parseDouble(v2Matcher.group(4)) : -1;

			if (v2Major > v1Major)
				return new SemInfo(SemLevel.MAJOR, (int) (v2Major - v1Major));
			if (v2Minor > v1Minor)
				return new SemInfo(SemLevel.MINOR, (int) (v2Minor - v1Minor));
			if (v2Patch > v1Patch)
				return new SemInfo(SemLevel.PATCH, (int) (v2Patch - v1Patch));
		}

		return new SemInfo(SemLevel.UNKNOWN, 0);
	}

	public static SemLevel computeExpectedLevel(int bcs, int additions, int modifications, int deprecated) {
		if (bcs > 0) // BCs => MAJOR
			return SemLevel.MAJOR;
		if (deprecated > 0) // @Deprecated => MINOR
			return SemLevel.MINOR;
		if (additions > 0) // New stuff => MINOR
			return SemLevel.MINOR;
		if (modifications == 0) // No API change => PATCH
			return SemLevel.PATCH;

		return SemLevel.UNKNOWN;
	}
	
	public static List<String> sortVersions(List<String> versions) {
		versions.sort((String v1, String v2) -> {
			return compare(v1, v2);
		});
		return versions;
	}

	public static int compare(String v1, String v2) {
		// Using maven-api as stated in the paper
		ArtifactVersion v1Artifact = new DefaultArtifactVersion(v1);
		ArtifactVersion v2Artifact = new DefaultArtifactVersion(v2);
		int comp = v1Artifact.compareTo(v2Artifact);
		return (comp < 0) ? -1 : (comp > 0) ? 1 : 0;
	}
}
