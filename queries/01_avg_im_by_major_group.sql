-- Average Importance (IM) by SOC Major Group
SELECT
  m.major_group_code,
  m.name AS major_group_name,
  ROUND(AVG(f.data_value), 2) AS avg_im,
  COUNT(*) AS rows_considered
FROM fact_occupation_element_rating f
JOIN dim_occupation d ON d.occupation_id = f.occupation_id
JOIN dim_major_group m ON m.major_group_code = d.major_group_code
JOIN dim_element e ON e.element_id = f.element_id
WHERE f.scale_id = 'IM'
GROUP BY m.major_group_code, m.name
ORDER BY avg_im DESC, m.major_group_code;

