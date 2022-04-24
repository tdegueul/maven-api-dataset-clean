package mcr;

import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.junit.Test;

import nl.cwi.swat.aethereal.AetherDownloader;

import static org.junit.Assert.*;

public class LanguageTests {
	private AetherDownloader downloader = new AetherDownloader(15);

	@Test
	public void testJava() {
		Artifact a = downloader.downloadArtifact(new DefaultArtifact("com.google.guava", "guava", "jar", "18.0"));
		JarStatistics s = new JarStatistics();
		s.compute(a.getFile().toPath());
		assertTrue(s.isJava());
		assertFalse(s.isKotlin());
		assertFalse(s.isClojure());
		assertFalse(s.isGroovy());
		assertFalse(s.isScala());
	}

	@Test
	public void testScala1() {
		Artifact a = downloader.downloadArtifact(new DefaultArtifact("com.typesafe.scala-logging", "scala-logging_2.13", "jar", "3.9.2"));
		JarStatistics s = new JarStatistics();
		s.compute(a.getFile().toPath());
		assertTrue(s.isScala());
		assertFalse(s.isKotlin());
		assertFalse(s.isClojure());
		assertFalse(s.isGroovy());
		assertFalse(s.isJava());
	}
	
	@Test
	public void testScala2() {
		Artifact a = downloader.downloadArtifact(new DefaultArtifact("com.typesafe.play", "play_2.13", "jar", "2.8.2"));
		JarStatistics s = new JarStatistics();
		s.compute(a.getFile().toPath());
		assertTrue(s.isScala());
		assertFalse(s.isKotlin());
		assertFalse(s.isClojure());
		assertFalse(s.isGroovy());
		assertFalse(s.isJava());
	}
	
	@Test
	public void testScala3() {
		Artifact a = downloader.downloadArtifact(new DefaultArtifact("com.typesafe.play", "play_2.10", "jar", "2.3.9"));
		JarStatistics s = new JarStatistics();
		s.compute(a.getFile().toPath());
		assertTrue(s.isScala());
		assertFalse(s.isKotlin());
		assertFalse(s.isClojure());
		assertFalse(s.isGroovy());
		assertFalse(s.isJava());
	}

	@Test
	public void testClojure2() {
		Artifact a = downloader.downloadArtifact(new DefaultArtifact("io.cucumber", "cucumber-clojure", "jar", "2.0.1"));
		JarStatistics s = new JarStatistics();
		s.compute(a.getFile().toPath());
		assertTrue(s.isClojure());
		assertFalse(s.isKotlin());
		assertFalse(s.isScala());
		assertFalse(s.isGroovy());
		assertFalse(s.isJava());
	}
	
	@Test
	public void testKotlin2() {
		Artifact a = downloader.downloadArtifact(new DefaultArtifact("com.github.manosbatsis.kotlin-utils", "kotlin-utils-api", "jar", "0.20"));
		JarStatistics s = new JarStatistics();
		s.compute(a.getFile().toPath());
		assertTrue(s.isKotlin());
		assertFalse(s.isClojure());
		assertFalse(s.isScala());
		assertFalse(s.isGroovy());
		assertFalse(s.isJava());
	}

	@Test
	public void testKotlin3() {
		Artifact a = downloader.downloadArtifact(new DefaultArtifact("com.squareup.moshi", "moshi-kotlin", "jar", "1.10.0"));
		JarStatistics s = new JarStatistics();
		s.compute(a.getFile().toPath());
		assertTrue(s.isKotlin());
		assertFalse(s.isClojure());
		assertFalse(s.isScala());
		assertFalse(s.isGroovy());
		assertFalse(s.isJava());
	}

	@Test
	public void testGroovy1() {
		Artifact a = downloader.downloadArtifact(new DefaultArtifact("com.netflix.rxjava", "rxjava-groovy", "jar", "0.20.7"));
		JarStatistics s = new JarStatistics();
		s.compute(a.getFile().toPath());
		assertTrue(s.isJava());
		assertFalse(s.isClojure());
		assertFalse(s.isScala());
		assertFalse(s.isKotlin());
		assertFalse(s.isGroovy());
	}

	@Test
	public void testGroovy2() {
		Artifact a = downloader.downloadArtifact(new DefaultArtifact("org.grails", "grails-core", "jar", "4.0.1"));
		JarStatistics s = new JarStatistics();
		s.compute(a.getFile().toPath());
		assertTrue(s.isGroovy());
		assertFalse(s.isClojure());
		assertFalse(s.isScala());
		assertFalse(s.isKotlin());
		assertFalse(s.isJava());
	}

	@Test
	public void testGroovy3() {
		Artifact a = downloader.downloadArtifact(new DefaultArtifact("com.gmongo", "gmongo", "jar", "1.5"));
		JarStatistics s = new JarStatistics();
		s.compute(a.getFile().toPath());
		assertTrue(s.isGroovy());
		assertFalse(s.isClojure());
		assertFalse(s.isScala());
		assertFalse(s.isKotlin());
		assertFalse(s.isJava());
	}
}
