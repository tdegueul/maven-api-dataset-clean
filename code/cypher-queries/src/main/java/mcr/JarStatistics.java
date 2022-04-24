package mcr;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.objectweb.asm.ClassReader;

public class JarStatistics {
	private int interfaces = 0;
	private int enums = 0;
	private int publicEnums = 0;
	private int classes = 0;
	private int publicClasses = 0;
	private int abstractClasses = 0;
	private int publicAbstractClasses = 0;
	private int methods = 0;
	private int publicMethods = 0;
	private int protectedMethods = 0;
	private int fields = 0;
	private int publicFields = 0;
	private int protectedFields = 0;
	private Map<Integer, Integer> versions = new HashMap<>();
	private Map<String, Integer> languages = new HashMap<>();

	public void compute(Path jar) {
		try (InputStream is = Files.newInputStream(jar); JarInputStream jarStream = new JarInputStream(is)) {
			JarEntry entry;
			while ((entry = jarStream.getNextJarEntry()) != null) {
				if (entry.getName().endsWith(".class")) {
					ClassStatisticsVisitor v = new ClassStatisticsVisitor();
					ClassReader cr = new ClassReader(jarStream);
					cr.accept(v, ClassReader.SKIP_CODE & ClassReader.SKIP_DEBUG & ClassReader.SKIP_FRAMES);

					if (v.isAnonymous())
						continue;

					if (v.isInterface())
						interfaces++;

					if (v.isEnum()) {
						enums++;
						if (v.isPublic())
							publicEnums++;
					}

					if (v.isClass()) {
						classes++;
						if (v.isAbstract())
							abstractClasses++;
						if (v.isPublic()) {
							publicClasses++;
							if (v.isAbstract())
								publicAbstractClasses++;
						}
					}

					methods += v.getMethods();
					publicMethods += v.getPublicMethods();
					protectedMethods += v.getProtectedMethods();
					fields += v.getFields();
					publicFields += v.getPublicFields();
					protectedFields += v.getProtectedFields();
					versions.put(v.getClassVersion(), versions.getOrDefault(v.getClassVersion(), 0) + 1);

					if (v.isJava())
						languages.put("java", languages.getOrDefault("java", 0) + 1);
					else if (v.isScala())
						languages.put("scala", languages.getOrDefault("scala", 0) + 1);
					else if (v.isGroovy())
						languages.put("groovy", languages.getOrDefault("groovy", 0) + 1);
					else if (v.isKotlin())
						languages.put("kotlin", languages.getOrDefault("kotlin", 0) + 1);
					else if (v.isClojure())
						languages.put("clojure", languages.getOrDefault("clojure", 0) + 1);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int getDeclarations() {
		return getTypes() + getMethods() + getFields();
	}

	public int getPublicDeclarations() {
		return getPublicTypes() + getPublicMethods() + getPublicFields();
	}

	public int getProtectedDeclarations() {
		return getProtectedMethods() + getProtectedFields();
	}

	public int getTypes() {
		return getClasses() + getInterfaces() + getEnums();
	}

	public int getPublicTypes() {
		return getPublicClasses() + getInterfaces() + getPublicEnums();
	}

	public int getAPIDeclarations() {
		return getPublicDeclarations() + getProtectedDeclarations();
	}

	public int getClasses() {
		return classes;
	}

	public int getPublicClasses() {
		return publicClasses;
	}

	public int getInterfaces() {
		return interfaces;
	}

	public int getAbstractClasses() {
		return abstractClasses;
	}

	public int getPublicAbstractClasses() {
		return publicAbstractClasses;
	}

	public int getEnums() {
		return enums;
	}

	public int getPublicEnums() {
		return publicEnums;
	}

	public int getMethods() {
		return methods;
	}

	public int getPublicMethods() {
		return publicMethods;
	}

	public int getProtectedMethods() {
		return protectedMethods;
	}

	public int getFields() {
		return fields;
	}

	public int getPublicFields() {
		return publicFields;
	}

	public int getProtectedFields() {
		return protectedFields;
	}

	public int getJavaVersion() {
		int max = 0;
		int maxValue = 0;

		for (Entry<Integer, Integer> e : versions.entrySet()) {
			if (e.getValue() > maxValue) {
				max = e.getKey();
				maxValue = e.getValue();
			}
		}

		return max;
	}

	public boolean isJava() {
		return languages.size() == 1 && languages.getOrDefault("java", 0) > 0;
	}

	public boolean isScala() {
		return languages.getOrDefault("scala", 0) > 0;
	}

	public boolean isKotlin() {
		return languages.getOrDefault("kotlin", 0) > 0;
	}

	public boolean isClojure() {
		return languages.getOrDefault("clojure", 0) > 0;
	}

	public boolean isGroovy() {
		return languages.getOrDefault("groovy", 0) > 0;
	}

	public boolean isMixed() {
		return languages.size() > 1;
	}

	public String getLanguage() {
		if (isJava())    return "java";
		if (isScala())   return "scala";
		if (isKotlin())  return "kotlin";
		if (isClojure()) return "clojure";
		if (isGroovy())  return "groovy";
		if (isMixed())   return "mixed";
		return "unknown";
	}

	public void printStats() {
		System.out.println(String.format(
				"D:%d PD:%d T:%d PT:%d C:%d PC:%d I:%d AC:%d PAC:%d E:%d PE: %d M:%d PM:%d F:%d PF:%d V:%s",
				getDeclarations(), getPublicDeclarations(), getTypes(), getPublicTypes(), getClasses(),
				getPublicClasses(), getInterfaces(), getAbstractClasses(), getPublicAbstractClasses(), getEnums(),
				getPublicEnums(), getMethods(), getPublicMethods(), getFields(), getPublicFields(), getJavaVersion()));
	}
}
