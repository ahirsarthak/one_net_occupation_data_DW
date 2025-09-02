-- Top 20 elements by average Importance (IM) across all occupations
SELECT
  e.domain,
  f.element_id,
  ROUND(AVG(f.data_value), 2) AS avg_im,
  COUNT(DISTINCT f.occupation_id) AS occupations_covered
FROM fact_occupation_element_rating f
JOIN dim_element e ON e.element_id = f.element_id
WHERE f.scale_id = 'IM'
GROUP BY e.domain, f.element_id
ORDER BY avg_im DESC, occupations_covered DESC
LIMIT 20;

