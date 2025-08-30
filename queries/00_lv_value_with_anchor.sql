-- LV values with mapped anchor description (rounded to nearest integer)
SELECT d.onetsoc_code, d.title, f.element_id,
       f.data_value AS lv_value, a.anchor_value, a.anchor_description
FROM fact_occupation_element_rating f
JOIN dim_occupation d ON d.occupation_id = f.occupation_id
JOIN dim_element_scale a
  ON a.element_id = f.element_id
 AND a.scale_id   = f.scale_id
 AND a.anchor_value = CAST(ROUND(f.data_value) AS INTEGER)
WHERE f.scale_id = 'LV';

