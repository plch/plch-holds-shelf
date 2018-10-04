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
data.item_record_location_code,
data.checkin_statistics_group_name,
data.pickup_location_code,
datetime(data.placed_epoch, 'unixepoch', 'localtime') as placed_datetime,
-- data.placed_epoch,
datetime(holds.modified_epoch, 'unixepoch', 'localtime') as modified_datetime,
-- holds.modified_epoch
(holds.modified_epoch - data.placed_epoch) / 86400  AS days_from_placed

FROM
holds

JOIN
data
ON
  data.hold_id = holds.hold_id
  AND data.modified_epoch = holds.modified_epoch