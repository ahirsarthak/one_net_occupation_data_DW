-- Average Importance (IM) by SOC Major Group (occupation-then-group average)
-- Computes each occupation's average IM across elements, then averages those per group.
SELECT
  m.major_group_code,
  m.name AS major_group_name,
  ROUND(AVG(o.avg_im), 2) AS avg_im_per_occ,
  COUNT(*) AS occupations_in_group
FROM (
  SELECT f.occupation_id, AVG(f.data_value) AS avg_im
  FROM fact_occupation_element_rating f
  WHERE f.scale_id = 'IM'
  GROUP BY f.occupation_id
) o
JOIN dim_occupation d ON d.occupation_id = o.occupation_id
JOIN dim_major_group m ON m.major_group_code = d.major_group_code
GROUP BY m.major_group_code, m.name
ORDER BY avg_im_per_occ DESC, m.major_group_code;
