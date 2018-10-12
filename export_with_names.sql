DROP TABLE IF EXISTS temp_on_holdshelf;

CREATE TEMPORARY TABLE temp_on_holdshelf AS
WITH holds AS (
	SELECT
	data.hold_id,
	MAX(data.modified_epoch) AS modified_epoch

	FROM
	data

	GROUP BY
	data.hold_id
)

SELECT
holds.hold_id,
data.record_type_code || data.record_num || 'a' AS record_num,

(
SELECT
locations.name
FROM
locations
WHERE
locations.code = data.s_location_code
) AS s_location_code,

(
SELECT
locations.name
FROM
locations
WHERE
locations.code = data.pickup_location_code
) AS pickup_location_code,

datetime(data.placed_epoch, 'unixepoch', 'localtime') as placed_datetime,

datetime(holds.modified_epoch, 'unixepoch', 'localtime') as modified_datetime,

(holds.modified_epoch - data.placed_epoch) / 86400  AS days_from_placed

FROM
holds

JOIN
data
ON
  data.hold_id = holds.hold_id
  AND data.modified_epoch = holds.modified_epoch
;

-- SELECT * FROM temp_on_holdshelf;

SELECT
t.s_location_code,
t.pickup_location_code,
count(t.pickup_location_code) as counted

FROM
temp_on_holdshelf as t

GROUP BY
t.s_location_code,
t.pickup_location_code
;
