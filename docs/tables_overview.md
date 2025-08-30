# Tables: Why These

This prototype keeps the model minimal and useful. Here’s why each table is included.

## stg_occupation_data
- Why: Raw landing preserves source fidelity for auditing and quick re‑runs.
- What: As‑is rows from O*NET Occupation Data (`onetsoc_code`, `title`, `description`).
- Use: Provenance for `dim_occupation` and sanity checks.

## dim_occupation
- Why: Clean hub for all occupation‑level analysis and joins.
- What: One row per occupation; derived `major_group_code` with FK to `dim_major_group`.
- Use: Drives counts by group, keyword searches, and future joins to skills/knowledge/abilities.

## dim_major_group
- Why: Translates 2‑digit SOC codes into readable names for reporting.
- What: Lookup from BLS; PK `major_group_code`, plus `code_full`, `name`.
- Use: Join from `dim_occupation.major_group_code` to show group names in results.

## dim_element
- Why: One place to register what’s being rated across domains (Skills/Knowledge/Abilities).
- What: `element_id` + `domain` ('SKILL' | 'KNOWLEDGE' | 'ABILITY').
- Use: Join target for all SKA ratings; lets you aggregate by element.

## dim_scale
- Why: Catalogs valid scales and basic metadata.
- What: `scale_id` (e.g., IM, LV), optional min/max.
- Use: Validates `scale_id` in facts/anchors; provides names.

## dim_element_scale
- Why: Anchor descriptions make level (LV) values human-readable.
- What: PK (`element_id`, `scale_id`, `anchor_value`), `anchor_description` (FKs to `dim_element` and `dim_scale`).
- Use: Join LV facts to rounded/nearest anchor for examples.

## fact_occupation_element_rating
- Why: Single generic fact for SKA ratings instead of three separate tables.
- What: Measures per `occupation + element + scale` (IM/LV), with metadata (n, SE, CI).
- Use: Top skills by group, highest occupations on a selected element. Join `dim_element` to get the element's domain when needed.

## Staging (for SKA)
- stg_skills, stg_knowledge, stg_abilities: raw landings to audit and transform into the fact.
- stg_level_scale_anchors: raw anchors to populate `dim_element_scale`.
- stg_scales_reference: raw scale list to populate `dim_scale` (IM/LV in this prototype).
- stg_invalid_ska: invalid/dropped SKA rows with `domain` and `error_reason`.
