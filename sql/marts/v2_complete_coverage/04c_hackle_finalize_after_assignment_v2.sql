/* ============================================================================
   Hackle 최종 복구 전용: 완성된 1,144만 event fact와 visit assignment CTAS를
   유지하고 assignment 인덱스, 30분 visit dimension, 호환 view, QA만 수행한다.
============================================================================ */
SET NAMES utf8mb4;
SET SESSION group_concat_max_len = 1024 * 1024;
CREATE DATABASE IF NOT EXISTS votes_mart;

SET @drop_finalize_visit_view_sql = (
    SELECT CASE TABLE_TYPE
        WHEN 'VIEW' THEN 'DROP VIEW votes_mart.vw_hackle_visit_session_24d_v2'
        WHEN 'BASE TABLE' THEN 'DROP TABLE votes_mart.vw_hackle_visit_session_24d_v2'
        ELSE 'DO 0'
    END
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA='votes_mart'
      AND TABLE_NAME='vw_hackle_visit_session_24d_v2'
    LIMIT 1
);
SET @drop_finalize_visit_view_sql=COALESCE(@drop_finalize_visit_view_sql,'DO 0');
PREPARE stmt_drop_finalize_visit_view FROM @drop_finalize_visit_view_sql;
EXECUTE stmt_drop_finalize_visit_view;
DEALLOCATE PREPARE stmt_drop_finalize_visit_view;

SET @drop_finalize_event_view_sql = (
    SELECT CASE TABLE_TYPE
        WHEN 'VIEW' THEN 'DROP VIEW votes_mart.mart_hackle_event_enriched_24d_v2'
        WHEN 'BASE TABLE' THEN 'DROP TABLE votes_mart.mart_hackle_event_enriched_24d_v2'
        ELSE 'DO 0'
    END
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA='votes_mart'
      AND TABLE_NAME='mart_hackle_event_enriched_24d_v2'
    LIMIT 1
);
SET @drop_finalize_event_view_sql=COALESCE(@drop_finalize_event_view_sql,'DO 0');
PREPARE stmt_drop_finalize_event_view FROM @drop_finalize_event_view_sql;
EXECUTE stmt_drop_finalize_event_view;
DEALLOCATE PREPARE stmt_drop_finalize_event_view;

DROP TABLE IF EXISTS votes_mart.dim_hackle_visit_30m_v2;

SET @assignment_index_sql = IF(
    EXISTS(
        SELECT 1
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA='votes_mart'
          AND TABLE_NAME='bridge_hackle_event_visit_assignment_v2'
          AND INDEX_NAME='PRIMARY'
    ),
    'DO 0',
    'ALTER TABLE votes_mart.bridge_hackle_event_visit_assignment_v2 ADD PRIMARY KEY (event_sk), ADD KEY idx_heva_visit30 (derived_visit_id_30m,event_order_in_visit_30m), ADD KEY idx_heva_partition15 (session_partition_sk,visit_sequence_15m), ADD KEY idx_heva_partition30 (session_partition_sk,visit_sequence_30m), ADD KEY idx_heva_partition60 (session_partition_sk,visit_sequence_60m), ADD KEY idx_heva_session (original_session_sk)'
);
PREPARE stmt_assignment_index FROM @assignment_index_sql;
EXECUTE stmt_assignment_index;
DEALLOCATE PREPARE stmt_assignment_index;

SELECT '[최종 복구] 방문 배정 인덱스 이후 단계 시작' AS build_progress;

/* --------------------------------------------------------------------------
   8. 기본 30분 방문 dimension. 방문 집계는 이벤트마다 반복 저장하지 않는다.
---------------------------------------------------------------------------- */
CREATE TABLE votes_mart.dim_hackle_visit_30m_v2 AS
SELECT
    a.derived_visit_id_30m,
    MIN(a.session_partition_sk) AS session_partition_sk,
    MIN(a.original_session_sk) AS original_session_sk,
    MIN(a.visit_sequence_30m) AS visit_sequence_30m,
    MIN(f.event_datetime_raw) AS visit_start_at_30m,
    MAX(f.event_datetime_raw) AS visit_end_at_30m,
    TIMESTAMPDIFF(SECOND,MIN(f.event_datetime_raw),MAX(f.event_datetime_raw))
        AS visit_duration_seconds_30m,
    COUNT(*) AS visit_event_count_30m,
    COUNT(DISTINCT f.event_key_attribute_sk) AS distinct_event_key_count,
    SUM(ek.attribute_value_raw='$session_start') AS session_start_event_count,
    SUM(ek.attribute_value_raw='launch_app') AS launch_app_count,
    SUM(ek.attribute_value_raw='click_question_start') AS question_start_count,
    SUM(ek.attribute_value_raw='complete_question') AS question_complete_count,
    SUM(ek.attribute_value_raw='skip_question') AS question_skip_count,
    SUM(ek.attribute_value_raw='open_ping') AS ping_open_count,
    SUM(ek.attribute_value_raw='view_shop') AS shop_view_count,
    SUM(ek.attribute_value_raw='click_purchase') AS purchase_click_count,
    SUM(ek.attribute_value_raw='complete_purchase') AS purchase_complete_count
FROM votes_mart.bridge_hackle_event_visit_assignment_v2 AS a
JOIN votes_mart.fact_hackle_event_24d_v2 AS f ON f.event_sk=a.event_sk
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS ek
  ON ek.text_attribute_sk=f.event_key_attribute_sk
GROUP BY a.derived_visit_id_30m;

ALTER TABLE votes_mart.dim_hackle_visit_30m_v2
    ADD PRIMARY KEY (derived_visit_id_30m),
    ADD KEY idx_hv30_session (original_session_sk),
    ADD KEY idx_hv30_start (visit_start_at_30m);


/* --------------------------------------------------------------------------
   9. 기존 이름·컬럼 호환 VIEW. 문자열은 조회할 때만 dimension에서 붙는다.
---------------------------------------------------------------------------- */
CREATE VIEW votes_mart.mart_hackle_event_enriched_24d_v2 AS
SELECT
    meta.mart_built_at,
    f.event_id,f.event_datetime_raw,ek.attribute_value_raw AS event_key,
    s.original_session_id,rid.attribute_value_raw AS raw_id_attribute,
    item.attribute_value_raw AS item_name_raw,
    page.attribute_value_raw AS page_name_raw,
    f.friend_count,f.votes_count,f.heart_balance,f.question_id,
    a.session_partition_sk AS session_partition_key,
    a.event_order_in_original_session,a.previous_event_at_in_original_session,
    a.seconds_since_previous_event,a.is_break_15m,a.is_break_30m,a.is_break_60m,
    a.visit_sequence_15m,a.visit_sequence_30m,a.visit_sequence_60m,
    LOWER(HEX(a.derived_visit_id_30m)) AS analytics_visit_session_id,
    a.event_order_in_visit_30m,v.visit_start_at_30m,v.visit_end_at_30m,
    v.visit_duration_seconds_30m,v.visit_event_count_30m,
    (a.event_order_in_visit_30m=1) AS is_visit_first_event_30m,
    (a.event_order_in_visit_30m=v.visit_event_count_30m) AS is_visit_last_event_30m,
    TIMESTAMPDIFF(SECOND,v.visit_start_at_30m,f.event_datetime_raw)
        AS seconds_from_visit_start_30m,
    s.property_row_count AS session_property_row_count,
    s.distinct_raw_user_id_count AS session_distinct_raw_user_id_count,
    s.resolved_raw_user_id,s.user_resolution_status AS session_user_resolution_status,
    s.user_conflict_flag AS session_user_conflict_flag,
    s.resolved_language AS session_language,s.resolved_osname AS session_osname,
    s.resolved_osversion AS session_osversion,
    s.resolved_versionname AS session_app_version,
    s.language_conflict_flag AS session_language_conflict_flag,
    s.osname_conflict_flag AS session_osname_conflict_flag,
    s.osversion_conflict_flag AS session_osversion_conflict_flag,
    s.versionname_conflict_flag AS session_versionname_conflict_flag,
    s.resolved_device_id,s.distinct_device_id_count AS session_distinct_device_id_count,
    s.device_conflict_flag AS session_device_conflict_flag,
    d.source_row_count AS device_property_row_count,
    d.distinct_model_count AS device_distinct_model_count,
    d.distinct_vendor_count AS device_distinct_vendor_count,
    d.resolved_device_model,d.resolved_device_vendor,
    d.model_conflict_flag AS device_model_conflict_flag,
    d.vendor_conflict_flag AS device_vendor_conflict_flag,
    u.hackle_user_property_user_id,u.hackle_user_property_class,
    u.hackle_user_property_gender,u.hackle_user_property_grade,
    u.hackle_user_property_school_id,u.hackle_user_property_match_flag,
    u.service_user_id,
    CASE
        WHEN s.user_resolution_status='NO_SESSION_PROPERTY' THEN 'NO_SESSION_PROPERTY'
        WHEN s.user_resolution_status='AMBIGUOUS_MULTIPLE_USER_IDS'
            THEN 'AMBIGUOUS_NOT_ASSIGNED'
        WHEN s.user_resolution_status='MISSING' THEN 'MISSING_USER_ID'
        ELSE u.account_identity_status
    END AS account_identity_status,
    (u.service_user_id IS NOT NULL) AS account_user_match_flag,
    u.current_account_gender,u.current_account_signup_at,
    u.current_account_is_staff,u.current_account_is_superuser,
    u.account_ban_status_current,u.current_account_group_id,
    u.account_grade_current,u.account_class_current,u.account_school_id_current,
    u.account_school_type_current,u.current_account_school_address,
    q.question_text,q.question_created_at AS question_master_created_at,
    (q.question_id IS NOT NULL) AS question_master_match_flag
FROM votes_mart.fact_hackle_event_24d_v2 AS f
JOIN votes_mart.bridge_hackle_event_visit_assignment_v2 AS a ON a.event_sk=f.event_sk
JOIN votes_mart.dim_hackle_visit_30m_v2 AS v
  ON v.derived_visit_id_30m=a.derived_visit_id_30m
JOIN votes_mart.dim_hackle_session_resolved_v2 AS s
  ON s.session_sk=f.original_session_sk
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS ek
  ON ek.text_attribute_sk=f.event_key_attribute_sk
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS rid
  ON rid.text_attribute_sk=f.raw_id_attribute_sk
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS item
  ON item.text_attribute_sk=f.item_name_attribute_sk
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS page
  ON page.text_attribute_sk=f.page_name_attribute_sk
LEFT JOIN votes_mart.dim_hackle_user_resolved_v2 AS u
  ON u.hackle_user_sk=s.resolved_hackle_user_sk
LEFT JOIN votes_mart.dim_hackle_device_resolved_v2 AS d
  ON d.device_sk=s.resolved_device_sk
LEFT JOIN votes_mart.dim_question_v2 AS q ON q.question_id=f.question_id
CROSS JOIN votes_mart.meta_hackle_build_v2 AS meta;


CREATE VIEW votes_mart.vw_hackle_visit_session_24d_v2 AS
SELECT
    LOWER(HEX(v.derived_visit_id_30m)) AS analytics_visit_session_id,
    s.original_session_id,v.visit_sequence_30m,
    v.visit_start_at_30m AS visit_start_at,v.visit_end_at_30m AS visit_end_at,
    v.visit_duration_seconds_30m AS visit_duration_seconds,
    v.visit_event_count_30m AS event_count,v.distinct_event_key_count,
    s.resolved_raw_user_id,u.service_user_id,
    CASE
        WHEN s.user_resolution_status='NO_SESSION_PROPERTY' THEN 'NO_SESSION_PROPERTY'
        WHEN s.user_resolution_status='AMBIGUOUS_MULTIPLE_USER_IDS'
            THEN 'AMBIGUOUS_NOT_ASSIGNED'
        WHEN s.user_resolution_status='MISSING' THEN 'MISSING_USER_ID'
        ELSE u.account_identity_status
    END AS account_identity_status,
    s.user_conflict_flag AS session_user_conflict_flag,
    s.resolved_device_id,s.device_conflict_flag AS session_device_conflict_flag,
    s.resolved_osname AS osname,s.resolved_osversion AS osversion,
    s.resolved_versionname AS app_version,d.resolved_device_model AS device_model,
    d.resolved_device_vendor AS device_vendor,
    u.hackle_user_property_gender AS hackle_gender,
    u.hackle_user_property_grade AS hackle_grade,
    u.hackle_user_property_class AS hackle_class,
    u.hackle_user_property_school_id AS hackle_school_id,
    u.account_school_id_current,
    v.session_start_event_count,v.launch_app_count,v.question_start_count,
    v.question_complete_count,v.question_skip_count,v.ping_open_count,
    v.shop_view_count,v.purchase_click_count,v.purchase_complete_count,
    (v.question_start_count>0) AS question_started_flag,
    (v.question_complete_count>0) AS question_completed_flag,
    (v.ping_open_count>0) AS ping_opened_flag,
    (v.shop_view_count>0) AS shop_viewed_flag,
    (v.purchase_click_count>0) AS purchase_clicked_flag,
    (v.purchase_complete_count>0) AS purchase_completed_flag
FROM votes_mart.dim_hackle_visit_30m_v2 AS v
JOIN votes_mart.dim_hackle_session_resolved_v2 AS s
  ON s.session_sk=v.original_session_sk
LEFT JOIN votes_mart.dim_hackle_user_resolved_v2 AS u
  ON u.hackle_user_sk=s.resolved_hackle_user_sk
LEFT JOIN votes_mart.dim_hackle_device_resolved_v2 AS d
  ON d.device_sk=s.resolved_device_sk;


/* --------------------------------------------------------------------------
   10. QA
---------------------------------------------------------------------------- */

/* QA_EXPECTED source=fact=distinct event_id=11,441,319 */
SELECT
    src.source_rows,fact.fact_rows,fact.distinct_event_ids,
    src.source_rows-fact.fact_rows AS source_minus_fact,
    fact.fact_rows-fact.distinct_event_ids AS duplicate_event_id_rows,
    CASE WHEN src.source_rows=fact.fact_rows
               AND fact.fact_rows=fact.distinct_event_ids
         THEN 'PASS' ELSE 'FAIL' END AS event_fact_grain_gate
FROM (SELECT COUNT(*) AS source_rows FROM final.hackle_events) AS src
CROSS JOIN (
    SELECT COUNT(*) AS fact_rows,COUNT(DISTINCT event_id) AS distinct_event_ids
    FROM votes_mart.fact_hackle_event_24d_v2
) AS fact;

/* 모든 event에 방문 배정이 정확히 1행이어야 한다. */
SELECT
    f.fact_rows,a.assignment_rows,a.distinct_event_sks,
    f.fact_rows-a.assignment_rows AS fact_minus_assignment,
    a.assignment_rows-a.distinct_event_sks AS duplicate_assignment_event_rows,
    CASE WHEN f.fact_rows=a.assignment_rows
               AND a.assignment_rows=a.distinct_event_sks
         THEN 'PASS' ELSE 'FAIL' END AS visit_assignment_gate
FROM (SELECT COUNT(*) AS fact_rows FROM votes_mart.fact_hackle_event_24d_v2) AS f
CROSS JOIN (
    SELECT COUNT(*) AS assignment_rows,COUNT(DISTINCT event_sk) AS distinct_event_sks
    FROM votes_mart.bridge_hackle_event_visit_assignment_v2
) AS a;

/* raw property 전행: 기대값 session=525,350 / device=252,380 */
SELECT 'hackle_properties' AS source_name,src.source_rows,b.bridge_rows,
       b.distinct_source_ids,
       CASE WHEN src.source_rows=b.bridge_rows
                  AND b.bridge_rows=b.distinct_source_ids
            THEN 'PASS' ELSE 'FAIL' END AS raw_bridge_gate
FROM (SELECT COUNT(*) AS source_rows FROM final.hackle_properties) AS src
CROSS JOIN (
    SELECT COUNT(*) AS bridge_rows,COUNT(DISTINCT property_row_id) AS distinct_source_ids
    FROM votes_mart.bridge_hackle_session_property_raw_v2
) AS b
UNION ALL
SELECT 'device_properties',src.source_rows,b.bridge_rows,b.distinct_source_ids,
       CASE WHEN src.source_rows=b.bridge_rows
                  AND b.bridge_rows=b.distinct_source_ids
            THEN 'PASS' ELSE 'FAIL' END
FROM (SELECT COUNT(*) AS source_rows FROM final.device_properties) AS src
CROSS JOIN (
    SELECT COUNT(*) AS bridge_rows,
           COUNT(DISTINCT device_property_row_id) AS distinct_source_ids
    FROM votes_mart.bridge_hackle_device_property_raw_v2
) AS b;

/* user_properties 전행 보존 */
SELECT
    src.source_rows,d.dim_rows,d.distinct_user_ids,
    CASE WHEN src.source_rows=d.dim_rows AND d.dim_rows=d.distinct_user_ids
         THEN 'PASS' ELSE 'FAIL' END AS user_property_gate
FROM (SELECT COUNT(*) AS source_rows FROM final.user_properties) AS src
CROSS JOIN (
    SELECT COUNT(*) AS dim_rows,COUNT(DISTINCT user_id) AS distinct_user_ids
    FROM votes_mart.dim_hackle_user_property_v2
) AS d;

/* conflict session/device를 강제 resolution하지 않는다. */
SELECT
    SUM(user_conflict_flag=1 AND resolved_hackle_user_sk IS NOT NULL)
        AS ambiguous_session_forced_user_assignments,
    SUM(device_conflict_flag=1 AND resolved_device_sk IS NOT NULL)
        AS ambiguous_session_forced_device_assignments,
    CASE WHEN SUM(user_conflict_flag=1 AND resolved_hackle_user_sk IS NOT NULL)=0
               AND SUM(device_conflict_flag=1 AND resolved_device_sk IS NOT NULL)=0
         THEN 'PASS' ELSE 'FAIL' END AS session_resolution_gate
FROM votes_mart.dim_hackle_session_resolved_v2;

SELECT
    SUM(model_conflict_flag=1 AND resolved_device_model IS NOT NULL)
        AS conflicting_model_forced_rows,
    SUM(vendor_conflict_flag=1 AND resolved_device_vendor IS NOT NULL)
        AS conflicting_vendor_forced_rows,
    CASE WHEN SUM(model_conflict_flag=1 AND resolved_device_model IS NOT NULL)=0
               AND SUM(vendor_conflict_flag=1 AND resolved_device_vendor IS NOT NULL)=0
         THEN 'PASS' ELSE 'FAIL' END AS device_resolution_gate
FROM votes_mart.dim_hackle_device_resolved_v2;

/* 방문 event 합계=fact */
SELECT
    f.fact_rows,v.visit_event_rows,
    f.fact_rows-v.visit_event_rows AS visit_event_count_difference,
    CASE WHEN f.fact_rows=v.visit_event_rows THEN 'PASS' ELSE 'FAIL' END
        AS visit_reconciliation_gate
FROM (SELECT COUNT(*) AS fact_rows FROM votes_mart.fact_hackle_event_24d_v2) AS f
CROSS JOIN (
    SELECT SUM(visit_event_count_30m) AS visit_event_rows
    FROM votes_mart.dim_hackle_visit_30m_v2
) AS v;

/* fact에 event_id 외 원시 profile/question/device 문자열이 없어야 한다. */
SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='votes_mart'
  AND TABLE_NAME='fact_hackle_event_24d_v2'
  AND DATA_TYPE IN ('char','varchar','tinytext','text','mediumtext','longtext')
ORDER BY ORDINAL_POSITION;

/* 실제 디스크 용량 */
SELECT
    TABLE_NAME,TABLE_ROWS AS estimated_rows,
    ROUND(DATA_LENGTH/1024/1024,1) AS data_mb,
    ROUND(INDEX_LENGTH/1024/1024,1) AS index_mb,
    ROUND((DATA_LENGTH+INDEX_LENGTH)/1024/1024,1) AS total_mb
FROM information_schema.TABLES
WHERE TABLE_SCHEMA='votes_mart' AND TABLE_TYPE='BASE TABLE'
  AND TABLE_NAME IN (
      'bridge_hackle_session_property_raw_v2',
      'bridge_hackle_device_property_raw_v2',
      'dim_hackle_user_property_v2',
      'dim_hackle_event_text_attribute_v2',
      'dim_hackle_device_resolved_v2',
      'dim_hackle_user_resolved_v2',
      'dim_hackle_session_resolved_v2',
      'fact_hackle_event_24d_v2',
      'bridge_hackle_event_visit_assignment_v2',
      'dim_hackle_visit_30m_v2'
  )
ORDER BY total_mb DESC;

/* 06/07 호환 컬럼과 모호 사용자 미귀속 최종 확인 */
SELECT
    COUNT(*) AS event_rows,COUNT(DISTINCT f.event_id) AS distinct_event_ids,
    COUNT(DISTINCT u.service_user_id) AS identified_service_users,
    COUNT(DISTINCT a.derived_visit_id_30m) AS derived_30m_visits,
    MIN(f.event_datetime_raw) AS min_event_at_raw,
    MAX(f.event_datetime_raw) AS max_event_at_raw,
    SUM(s.user_conflict_flag=1 AND u.service_user_id IS NOT NULL)
        AS ambiguous_session_forced_user_assignments
FROM votes_mart.fact_hackle_event_24d_v2 AS f
JOIN votes_mart.bridge_hackle_event_visit_assignment_v2 AS a
  ON a.event_sk=f.event_sk
JOIN votes_mart.dim_hackle_session_resolved_v2 AS s
  ON s.session_sk=f.original_session_sk
LEFT JOIN votes_mart.dim_hackle_user_resolved_v2 AS u
  ON u.hackle_user_sk=s.resolved_hackle_user_sk;


