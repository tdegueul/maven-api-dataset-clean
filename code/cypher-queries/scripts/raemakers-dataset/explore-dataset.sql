-- Number of JARs
SELECT COUNT(*)
FROM raemakers.files;


-- Number of JARs with null version
SELECT COUNT(*)
FROM raemakers.files
WHERE version IS NULL OR version = '';


-- Unique APIs
SELECT 
    files.groupId,
    files.artifactId,
    files.version,
    COUNT(*) AS records
FROM raemakers.files AS files
WHERE files.version IS NOT NULL
	AND files.version <> ''
GROUP BY files.groupId , files.artifactId , files.version
HAVING COUNT(*) > 1;


-- Number of unique APIs
SELECT COUNT(*)
FROM
    (SELECT 
		files.groupId,
		files.artifactId,
		files.version,
		COUNT(*) AS records
    FROM raemakers.files AS files
    WHERE files.version IS NOT NULL
		AND files.version <> ''
    GROUP BY files.groupId , files.artifactId , files.version) AS `unique`;
    

-- Unique APIs following semver
SELECT DISTINCT
    files.groupId AS `group`,
    files.artifactId AS artifact,
    files.version AS version
FROM raemakers.files AS files
WHERE files.version REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$';


-- Number of unique APIs following semver
SELECT COUNT(*)
FROM (
	SELECT DISTINCT
		files.groupId,
		files.artifactId,
		files.version
	FROM raemakers.files AS files
	WHERE files.version REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$'
) AS apis;


-- Unique APIs NOT following semver
SELECT DISTINCT
    files.groupId,
    files.artifactId,
    files.version
FROM raemakers.files AS files
WHERE files.version NOT REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$';


-- Number of unique APIs NOT following semver
SELECT COUNT(*)
FROM (
	SELECT DISTINCT
		files.groupId,
		files.artifactId,
		files.version
	FROM raemakers.files AS files
	WHERE files.version NOT REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$'
) AS apis;


-- APIs with multiple records
SELECT 
    files.fileId AS id,
    files.groupId AS `group`,
    files.artifactId AS artifact,
    files.version AS version
FROM raemakers.files AS files
INNER JOIN
    (SELECT 
        files.groupId,
        files.artifactId,
        files.version,
        COUNT(*) AS records
    FROM raemakers.files AS files
    WHERE files.version IS NOT NULL
		AND files.version <> ''
    GROUP BY files.groupId , files.artifactId , files.version
    HAVING COUNT(*) > 1) AS uniq 
	ON files.groupId = uniq.groupId
		AND files.artifactId = uniq.artifactId
		AND files.version = uniq.version;


-- Number of all changes
SELECT COUNT(*)
FROM raemakers.changes;


-- Upgrades
SELECT DISTINCT fileIdv1, fileIdv2
FROM raemakers.changes;


-- Number of upgrades
SELECT COUNT(*)
FROM
    (SELECT DISTINCT fileIdv1, fileIdv2
    FROM raemakers.changes) AS changes;


-- Raw upgrades
SELECT 
    interm.`group` AS `group`,
    interm.artifact AS artifact,
    v1files.version AS v1,
    v2files.version AS v2
FROM
    (SELECT
        files.groupId AS `group`,
        files.artifactId AS artifact,
        changes.fileIdv1 AS idv1,
        changes.fileIdv2 AS idv2
    FROM raemakers.files AS files
    INNER JOIN (
		SELECT DISTINCT c.fileIdv1, c.fileIdv2
		FROM raemakers.changes AS c) AS changes 
	ON files.fileId = changes.fileIdv1) AS interm
	INNER JOIN
		raemakers.files AS v1files ON v1files.fileId = interm.idv1
	INNER JOIN
		raemakers.files AS v2files ON v2files.fileId = interm.idv2;


-- Number of raw upgrades
SELECT COUNT(*)
FROM
    (SELECT 
        interm.`group` AS `group`,
        interm.artifact AS artifact,
        v1files.version AS v1,
        v2files.version AS v2
    FROM
        (SELECT
			files.groupId AS `group`,
            files.artifactId AS artifact,
            changes.fileIdv1 AS idv1,
            changes.fileIdv2 AS idv2
		FROM raemakers.files AS files
		INNER JOIN 
			(SELECT DISTINCT
				c.fileIdv1, c.fileIdv2
			FROM raemakers.changes AS c) AS changes 
		ON files.fileId = changes.fileIdv1) AS interm
    INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1
    INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2) AS upgrades;


-- Verify uniqueness of v1
-- Are there multiple records with the same group, artifact and version?
SELECT 
    upgrades.`group`,
    upgrades.artifact,
    upgrades.v1,
    COUNT(*) AS records
FROM
    (SELECT 
        interm.`group` AS `group`,
		interm.artifact AS artifact,
		v1files.version AS v1,
		v2files.version AS v2
    FROM
        (SELECT
			files.groupId AS `group`,
            files.artifactId AS artifact,
            changes.fileIdv1 AS idv1,
            changes.fileIdv2 AS idv2
    FROM raemakers.files AS files
    INNER JOIN 
		(SELECT DISTINCT
			c.fileIdv1, c.fileIdv2
		FROM raemakers.changes AS c) AS changes 
	ON files.fileId = changes.fileIdv1) AS interm
    INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1
    INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2) AS upgrades
GROUP BY upgrades.`group` , upgrades.artifact , upgrades.v1
HAVING COUNT(*) > 1;


-- Check non-unique records
-- List records where v1 appears more than once
SELECT DISTINCT
    upgrades.`group`,
    upgrades.artifact,
    upgrades.v1,
    upgrades.v2
FROM
    (SELECT 
        upgrades.`group`,
		upgrades.artifact,
		upgrades.v1,
		COUNT(*) AS records
    FROM
        (SELECT 
			interm.`group` AS `group`,
            interm.artifact AS artifact,
            v1files.version AS v1,
            v2files.version AS v2
		FROM
			(SELECT
				files.groupId AS `group`,
				files.artifactId AS artifact,
				changes.fileIdv1 AS idv1,
				changes.fileIdv2 AS idv2
			FROM raemakers.files AS files
			INNER JOIN 
				(SELECT DISTINCT
					c.fileIdv1, c.fileIdv2
				FROM raemakers.changes AS c) AS changes 
			ON files.fileId = changes.fileIdv1) AS interm
		INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1
		INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2) AS upgrades
		GROUP BY upgrades.`group` , upgrades.artifact , upgrades.v1
		HAVING COUNT(*) > 1) AS nonunique
	INNER JOIN
		(SELECT 
			interm.`group` AS `group`,
            interm.artifact AS artifact,
            v1files.version AS v1,
            v2files.version AS v2
		FROM
			(SELECT
				files.groupId AS `group`,
				files.artifactId AS artifact,
				changes.fileIdv1 AS idv1,
				changes.fileIdv2 AS idv2
			FROM raemakers.files AS files
			INNER JOIN 
				(SELECT DISTINCT
					c.fileIdv1, c.fileIdv2
				FROM raemakers.changes AS c) AS changes 
			ON files.fileId = changes.fileIdv1) AS interm
		INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1
		INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2) AS upgrades 
        ON nonunique.`group` = upgrades.`group`
			AND nonunique.artifact = upgrades.artifact
			AND nonunique.v1 = upgrades.v1;


-- Number of non-unique records (records that repeat v1) 
-- Exclude one record pointing to v1 (COUNT(*)-1)
SELECT SUM(records)
FROM
    (SELECT 
        upgrades.`group`,
		upgrades.artifact,
		upgrades.v1,
		COUNT(*) - 1 AS records
    FROM
        (SELECT 
			interm.`group` AS `group`,
            interm.artifact AS artifact,
            v1files.version AS v1,
            v2files.version AS v2
		FROM
			(SELECT 
				files.groupId AS `group`,
				files.artifactId AS artifact,
				changes.fileIdv1 AS idv1,
				changes.fileIdv2 AS idv2
			FROM raemakers.files AS files
			INNER JOIN 
				(SELECT DISTINCT
					c.fileIdv1, c.fileIdv2
				FROM raemakers.changes AS c) AS changes 
			ON files.fileId = changes.fileIdv1) AS interm
		INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1
		INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2) AS upgrades
		GROUP BY upgrades.`group` , upgrades.artifact , upgrades.v1
		HAVING COUNT(*) > 1
	) AS nonunique;
    

-- Unwanted (v1 == v2) pairs from upgrades
-- NOTE: non-unique records respond to cases where v1 == v2
SELECT 
	interm.`group` AS `group`,
	interm.artifact AS artifact,
	v1files.version AS v1,
	v2files.version AS v2
FROM
	(SELECT
		files.groupId AS `group`,
		files.artifactId AS artifact,
		changes.fileIdv1 AS idv1,
		changes.fileIdv2 AS idv2
	FROM raemakers.files AS files
	INNER JOIN 
		(SELECT DISTINCT
			c.fileIdv1, c.fileIdv2
		FROM raemakers.changes AS c) AS changes 
	ON files.fileId = changes.fileIdv1) AS interm
INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1    
INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2
HAVING v1 = v2;


-- Number of unwanted (v1 == v2) pairs from upgrades
SELECT COUNT(*)
FROM
    (SELECT 
	interm.`group` AS `group`,
	interm.artifact AS artifact,
	v1files.version AS v1,
	v2files.version AS v2
	FROM
		(SELECT
			files.groupId AS `group`,
			files.artifactId AS artifact,
			changes.fileIdv1 AS idv1,
			changes.fileIdv2 AS idv2
		FROM raemakers.files AS files
	INNER JOIN 
		(SELECT DISTINCT
			c.fileIdv1, c.fileIdv2
		FROM raemakers.changes AS c) AS changes 
	ON files.fileId = changes.fileIdv1) AS interm
INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1    
INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2
HAVING v1 = v2) AS unwanted;


-- Unwanted pairs from upgrades where v1 or v2 do not follow semver versioning
-- We consider pairs after filtering cases where v1 == v2
SELECT 
	interm.`group` AS `group`,
	interm.artifact AS artifact,
	v1files.version AS v1,
	v2files.version AS v2
FROM
	(SELECT
		files.groupId AS `group`,
		files.artifactId AS artifact,
		changes.fileIdv1 AS idv1,
		changes.fileIdv2 AS idv2
	FROM raemakers.files AS files
	INNER JOIN 
		(SELECT DISTINCT
			c.fileIdv1, c.fileIdv2
		FROM raemakers.changes AS c) AS changes 
	ON files.fileId = changes.fileIdv1) AS interm
INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1
INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2
HAVING v1 <> v2
	AND (v1 NOT REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$'
		OR v2 NOT REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$');


-- Number of unwanted (v1 and v2 do not follow semver versions) pairs from upgrades
SELECT COUNT(*)
FROM
    (SELECT 
		interm.`group` AS `group`,
		interm.artifact AS artifact,
		v1files.version AS v1,
		v2files.version AS v2
	FROM
		(SELECT
			files.groupId AS `group`,
			files.artifactId AS artifact,
			changes.fileIdv1 AS idv1,
			changes.fileIdv2 AS idv2
		FROM raemakers.files AS files
		INNER JOIN 
			(SELECT DISTINCT
				c.fileIdv1, c.fileIdv2
			FROM raemakers.changes AS c) AS changes 
		ON files.fileId = changes.fileIdv1) AS interm
	INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1
	INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2
	HAVING v1 <> v2
		AND (v1 NOT REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$'
			OR v2 NOT REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$')) AS unwanted;


-- Removing unwanted (v1 == v2) pairs from upgrades 
-- Only considering semver versions: d+.d+(.d+)?
SELECT 
	interm.`group` AS `group`,
	interm.artifact AS artifact,
	v2files.version AS v1,
    v1files.version AS v2 -- Invert versions (more cases represented in this way)
FROM
	(SELECT
		files.groupId AS `group`,
		files.artifactId AS artifact,
		changes.fileIdv1 AS idv1,
		changes.fileIdv2 AS idv2
	FROM raemakers.files AS files
	INNER JOIN 
		(SELECT DISTINCT
			c.fileIdv1, c.fileIdv2
		FROM raemakers.changes AS c) AS changes 
	ON files.fileId = changes.fileIdv1) AS interm
INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1
INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2
HAVING v1 <> v2
	AND v1 REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$'
	AND v2 REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$';


-- Number of upgrades 
SELECT COUNT(*)
FROM
    (SELECT 
		interm.`group` AS `group`,
		interm.artifact AS artifact,
        v2files.version AS v1,
		v1files.version AS v2 -- Invert versions (more cases represented in this way)
	FROM
		(SELECT
			files.groupId AS `group`,
			files.artifactId AS artifact,
			changes.fileIdv1 AS idv1,
			changes.fileIdv2 AS idv2
		FROM raemakers.files AS files
		INNER JOIN 
			(SELECT DISTINCT
				c.fileIdv1, c.fileIdv2
			FROM raemakers.changes AS c) AS changes 
		ON files.fileId = changes.fileIdv1) AS interm
	INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1
	INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2
	HAVING v1 <> v2
		AND v1 REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$'
		AND v2 REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$') AS upgrades;


-- APIs remaining after filtering
SELECT DISTINCT
	upgrades1.`group`,
	upgrades1.artifact,
    upgrades1.v1
FROM
    (SELECT 
		interm.`group` AS `group`,
		interm.artifact AS artifact,
        v2files.version AS v1,
		v1files.version AS v2 -- Invert versions (more cases represented in this way)
	FROM
		(SELECT
			files.groupId AS `group`,
			files.artifactId AS artifact,
			changes.fileIdv1 AS idv1,
			changes.fileIdv2 AS idv2
		FROM raemakers.files AS files
		INNER JOIN 
			(SELECT DISTINCT
				c.fileIdv1, c.fileIdv2
			FROM raemakers.changes AS c) AS changes 
		ON files.fileId = changes.fileIdv1) AS interm
	INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1
	INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2
	HAVING v1 <> v2
		AND v1 REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$'
		AND v2 REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$') AS upgrades1
UNION
SELECT DISTINCT
	upgrades2.`group`,
	upgrades2.artifact,
    upgrades2.v2
FROM
    (SELECT 
		interm.`group` AS `group`,
		interm.artifact AS artifact,
        v2files.version AS v1,
		v1files.version AS v2 -- Invert versions (more cases represented in this way)
	FROM
		(SELECT
			files.groupId AS `group`,
			files.artifactId AS artifact,
			changes.fileIdv1 AS idv1,
			changes.fileIdv2 AS idv2
		FROM raemakers.files AS files
		INNER JOIN 
			(SELECT DISTINCT
				c.fileIdv1, c.fileIdv2
			FROM raemakers.changes AS c) AS changes 
		ON files.fileId = changes.fileIdv1) AS interm
	INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1
	INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2
	HAVING v1 <> v2
		AND v1 REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$'
		AND v2 REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$') AS upgrades2;
        
	
-- APIs remaining after filtering
SELECT COUNT(*)
FROM
	(SELECT DISTINCT
		upgrades1.`group`,
		upgrades1.artifact,
		upgrades1.v1
	FROM
		(SELECT 
			interm.`group` AS `group`,
			interm.artifact AS artifact,
			v2files.version AS v1,
			v1files.version AS v2 -- Invert versions (more cases represented in this way)
		FROM
			(SELECT
				files.groupId AS `group`,
				files.artifactId AS artifact,
				changes.fileIdv1 AS idv1,
				changes.fileIdv2 AS idv2
			FROM raemakers.files AS files
			INNER JOIN 
				(SELECT DISTINCT
					c.fileIdv1, c.fileIdv2
				FROM raemakers.changes AS c) AS changes 
			ON files.fileId = changes.fileIdv1) AS interm
		INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1
		INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2
		HAVING v1 <> v2
			AND v1 REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$'
			AND v2 REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$') AS upgrades1
	UNION
	SELECT DISTINCT
		upgrades2.`group`,
		upgrades2.artifact,
		upgrades2.v2
	FROM
		(SELECT 
			interm.`group` AS `group`,
			interm.artifact AS artifact,
			v2files.version AS v1,
			v1files.version AS v2 -- Invert versions (more cases represented in this way)
		FROM
			(SELECT
				files.groupId AS `group`,
				files.artifactId AS artifact,
				changes.fileIdv1 AS idv1,
				changes.fileIdv2 AS idv2
			FROM raemakers.files AS files
			INNER JOIN 
				(SELECT DISTINCT
					c.fileIdv1, c.fileIdv2
				FROM raemakers.changes AS c) AS changes 
			ON files.fileId = changes.fileIdv1) AS interm
		INNER JOIN raemakers.files AS v1files ON v1files.fileId = interm.idv1
		INNER JOIN raemakers.files AS v2files ON v2files.fileId = interm.idv2
		HAVING v1 <> v2
			AND v1 REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$'
			AND v2 REGEXP '^[0-9]+[.][0-9]+([.][0-9]+)?$') AS upgrades2
) AS apis;