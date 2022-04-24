package mcr.data;

import java.util.Map;

public class Upgrade {
	public final String lgroup;
	public final String lartifact;
	public final String lv1;
	public final String lv2;
	public final String level;
	public final int year;
	public final String ljarV1;
	public final String ljarV2;
	public final String cgroup;
	public final String cartifact;
	public final String cv;
	public final int cyear;
	public final String delta;

	public Upgrade(String lgroup, String lartifact, String lv1,
		String lv2, String level, int year, String ljarV1, String ljarV2,
		String cgroup, String cartifact, String cv, int cyear, String delta) {
		this.lgroup    = lgroup;
		this.lartifact = lartifact;
		this.lv1       = lv1;
		this.lv2       = lv2;
		this.level     = level;
		this.year      = year;
		this.ljarV1    = ljarV1;
		this.ljarV2    = ljarV2;
		this.cgroup    = cgroup;
		this.cartifact = cartifact;
		this.cv        = cv;
		this.cyear     = cyear;
		this.delta     = delta;
	}

	public static Upgrade fromCsv(Map<String, String> line) {
		return new Upgrade(
			line.get("lgroup"),
			line.get("lartifact"),
			line.get("lv1"),
			line.get("lv2"),
			line.get("level"),
			Integer.parseInt(line.get("year")),
			line.get("jar_v1"),
			line.get("jar_v2"),
			line.get("cgroup"),
			line.get("cartifact"),
			line.get("cversion"),
			Integer.parseInt(line.get("cyear")),
			line.get("delta"));
	}
}
