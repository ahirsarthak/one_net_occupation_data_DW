-- Top occupations for a chosen element (show IM and LV side-by-side)
-- Change the element_id below as needed
-- Example: '1.A.1.b.4'
SELECT
  d.onetsoc_code,
  d.title,
  ROUND(MAX(CASE WHEN f.scale_id = 'IM' THEN f.data_value END), 2) AS im_value,
  ROUND(MAX(CASE WHEN f.scale_id = 'LV' THEN f.data_value END), 2) AS lv_value
FROM fact_occupation_element_rating f
JOIN dim_occupation d ON d.occupation_id = f.occupation_id
WHERE f.element_id = '1.A.1.b.4'
GROUP BY d.onetsoc_code, d.title
ORDER BY im_value DESC NULLS LAST, lv_value DESC NULLS LAST
LIMIT 25;

