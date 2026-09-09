/* ============================================================================
   Hackle 복구 전용: 0~3단계에서 이미 완성된 원시 bridge/dim/text 사전을 재사용하고
   device/user/session dimension부터 다시 구축한다.
   표준 전체 재구축 파일은 04_hackle_event_enriched_24d_v2.sql이다.
============================================================================ */
SET NAMES utf8mb4;
SET SESSION group_concat_max_len = 1024 * 1024;
CREATE DATABASE IF NOT EXISTS votes_mart;

SET @drop_resume_view_sql = (
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
SET @drop_resume_view_sql=COALESCE(@drop_resume_view_sql,'DO 0');
PREPARE stmt_drop_resume_view FROM @drop_resume_view_sql;
EXECUTE stmt_drop_resume_view;
DEALLOCATE PREPARE stmt_drop_resume_view;

SET @drop_resume_mart_sql = (
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
SET @drop_resume_mart_sql=COALESCE(@drop_resume_mart_sql,'DO 0');
PREPARE stmt_drop_resume_mart FROM @drop_resume_mart_sql;
EXECUTE stmt_drop_resume_mart;
DEALLOCATE PREPARE stmt_drop_resume_mart;

DROP TABLE IF EXISTS votes_mart.dim_hackle_visit_30m_v2;
DROP TABLE IF EXISTS votes_mart.bridge_hackle_event_visit_assignment_v2;
DROP TABLE IF EXISTS votes_mart.fact_hackle_event_24d_v2;
DROP TABLE IF EXISTS votes_mart.dim_hackle_session_resolved_v2;
DROP TABLE IF EXISTS votes_mart.dim_hackle_user_resolved_v2;
DROP TABLE IF EXISTS votes_mart.dim_hackle_device_resolved_v2;

SELECT '[복구] 완성된 원시 bridge/dim/text 사전을 유지하고 4단계부터 재개' AS build_progress;

/* --------------------------------------------------------------------------
   4. device/user resolved dimensions
---------------------------------------------------------------------------- */
CREATE TABLE votes_mart.dim_hackle_device_resolved_v2 AS
WITH device_rollup AS (
    SELECT
        normalized_device_id,
        COUNT(*) AS source_row_count,
        COUNT(DISTINCT NULLIF(TRIM(device_model_raw),'')) AS distinct_model_count,
        COUNT(DISTINCT NULLIF(TRIM(device_vendor_raw),'')) AS distinct_vendor_count,
        CASE WHEN COUNT(DISTINCT NULLIF(TRIM(device_model_raw),''))=1
             THEN MAX(NULLIF(TRIM(device_model_raw),'')) END AS resolved_device_model,
        CASE WHEN COUNT(DISTINCT NULLIF(TRIM(device_vendor_raw),''))=1
             THEN MAX(NULLIF(TRIM(device_vendor_raw),'')) END AS resolved_device_vendor
    FROM votes_mart.bridge_hackle_device_property_raw_v2
    WHERE normalized_device_id IS NOT NULL
    GROUP BY normalized_device_id
)
SELECT
    ROW_NUMBER() OVER(ORDER BY normalized_device_id) AS device_sk,
    r.*,
    (r.distinct_model_count>1) AS model_conflict_flag,
    (r.distinct_vendor_count>1) AS vendor_conflict_flag
FROM device_rollup AS r;

ALTER TABLE votes_mart.dim_hackle_device_resolved_v2
    ADD PRIMARY KEY (device_sk),
    ADD UNIQUE KEY uk_hdr_device_id (normalized_device_id);


CREATE TABLE votes_mart.dim_hackle_user_resolved_v2 AS
WITH raw_user_key AS (
    SELECT normalized_raw_user_id
    FROM votes_mart.bridge_hackle_session_property_raw_v2
    WHERE normalized_raw_user_id IS NOT NULL
    GROUP BY normalized_raw_user_id
    UNION
    SELECT NULLIF(TRIM(user_id),'')
    FROM votes_mart.dim_hackle_user_property_v2
    WHERE NULLIF(TRIM(user_id),'') IS NOT NULL
),
keyed AS (
    SELECT
        ROW_NUMBER() OVER(ORDER BY normalized_raw_user_id) AS hackle_user_sk,
        normalized_raw_user_id,
        CASE WHEN normalized_raw_user_id REGEXP '^[0-9]+$'
             THEN 'NUMERIC' ELSE 'NONNUMERIC' END AS raw_user_id_format,
        CASE WHEN normalized_raw_user_id REGEXP '^[0-9]+$'
             THEN CAST(normalized_raw_user_id AS UNSIGNED) END
            AS service_user_id_candidate
    FROM raw_user_key
),
user_property_rollup AS (
    /* 원문 user_id가 공백 차이만으로 둘 이상 존재하더라도 fan-out시키지 않는다.
       충돌한 정규화 키는 원시 bridge/dim에는 모두 보존하되 프로필은 미귀속한다. */
    SELECT
        NULLIF(TRIM(user_id),'') AS normalized_raw_user_id,
        COUNT(*) AS source_row_count,
        CASE WHEN COUNT(*)=1 THEN MAX(user_id) END AS user_id,
        CASE WHEN COUNT(*)=1 THEN MAX(class_raw) END AS class_raw,
        CASE WHEN COUNT(*)=1 THEN MAX(gender_raw) END AS gender_raw,
        CASE WHEN COUNT(*)=1 THEN MAX(grade_raw) END AS grade_raw,
        CASE WHEN COUNT(*)=1 THEN MAX(school_id_raw) END AS school_id_raw
    FROM votes_mart.dim_hackle_user_property_v2
    WHERE NULLIF(TRIM(user_id),'') IS NOT NULL
    GROUP BY NULLIF(TRIM(user_id),'')
)
SELECT
    k.hackle_user_sk,k.normalized_raw_user_id,k.raw_user_id_format,
    k.service_user_id_candidate,au.id AS service_user_id,
    CASE
        WHEN k.raw_user_id_format='NONNUMERIC'
            THEN 'UNIQUE_NONNUMERIC_NOT_ACCOUNT_ASSIGNED'
        WHEN au.id IS NULL THEN 'UNIQUE_NUMERIC_ACCOUNT_NOT_FOUND'
        ELSE 'UNIQUE_NUMERIC_ACCOUNT_MATCHED'
    END AS account_identity_status,
    up.user_id AS hackle_user_property_user_id,
    up.class_raw AS hackle_user_property_class,
    up.gender_raw AS hackle_user_property_gender,
    up.grade_raw AS hackle_user_property_grade,
    up.school_id_raw AS hackle_user_property_school_id,
    (up.user_id IS NOT NULL) AS hackle_user_property_match_flag,
    COALESCE(up.source_row_count,0) AS hackle_user_property_source_row_count,
    (COALESCE(up.source_row_count,0)>1) AS hackle_user_property_key_conflict_flag,
    au.gender AS current_account_gender,
    au.created_at AS current_account_signup_at,
    au.is_staff AS current_account_is_staff,
    au.is_superuser AS current_account_is_superuser,
    au.ban_status AS account_ban_status_current,
    au.group_id AS current_account_group_id,
    ag.grade AS account_grade_current,
    ag.class_num AS account_class_current,
    ag.school_id AS account_school_id_current,
    s.school_type AS account_school_type_current,
    s.address AS current_account_school_address,
    'HACKLE_ID_SAFE_RESOLUTION_CURRENT_ACCOUNT_PROFILE' AS identity_scope
FROM keyed AS k
LEFT JOIN user_property_rollup AS up
  ON up.normalized_raw_user_id=k.normalized_raw_user_id
LEFT JOIN final.accounts_user AS au ON au.id=k.service_user_id_candidate
LEFT JOIN final.accounts_group AS ag ON ag.id=au.group_id
LEFT JOIN final.accounts_school AS s ON s.id=ag.school_id;

ALTER TABLE votes_mart.dim_hackle_user_resolved_v2
    ADD PRIMARY KEY (hackle_user_sk),
    ADD UNIQUE KEY uk_hur_raw_user (normalized_raw_user_id),
    ADD KEY idx_hur_service_user (service_user_id),
    ADD KEY idx_hur_hackle_school (hackle_user_property_school_id),
    ADD KEY idx_hur_account_school (account_school_id_current);


/* --------------------------------------------------------------------------
   5. original session + conflict-safe resolved dimension
---------------------------------------------------------------------------- */
CREATE TABLE votes_mart.dim_hackle_session_resolved_v2 AS
WITH raw_session AS (
    SELECT session_id AS original_session_id
    FROM final.hackle_events GROUP BY session_id
    UNION
    SELECT original_session_id_raw
    FROM votes_mart.bridge_hackle_session_property_raw_v2
    GROUP BY original_session_id_raw
),
session_keyed AS (
    SELECT
        ROW_NUMBER() OVER(
            ORDER BY CASE WHEN original_session_id IS NULL THEN 0
                          WHEN TRIM(original_session_id)='' THEN 1 ELSE 2 END,
                     original_session_id
        ) AS session_sk,
        original_session_id,
        NULLIF(TRIM(original_session_id),'') AS normalized_session_id,
        CASE WHEN original_session_id IS NULL THEN 'NULL'
             WHEN TRIM(original_session_id)='' THEN 'EMPTY_OR_WHITESPACE'
             ELSE 'VALUE' END AS original_session_id_state
    FROM raw_session
),
property_rollup AS (
    SELECT
        original_session_id_raw,
        COUNT(*) AS property_row_count,
        COUNT(DISTINCT normalized_raw_user_id) AS distinct_raw_user_id_count,
        COUNT(DISTINCT NULLIF(TRIM(language_raw),'')) AS distinct_language_count,
        COUNT(DISTINCT NULLIF(TRIM(osname_raw),'')) AS distinct_osname_count,
        COUNT(DISTINCT NULLIF(TRIM(osversion_raw),'')) AS distinct_osversion_count,
        COUNT(DISTINCT NULLIF(TRIM(versionname_raw),'')) AS distinct_versionname_count,
        COUNT(DISTINCT normalized_device_id) AS distinct_device_id_count,
        CASE WHEN COUNT(DISTINCT normalized_raw_user_id)=1
             THEN MAX(normalized_raw_user_id) END AS resolved_raw_user_id,
        CASE WHEN COUNT(DISTINCT NULLIF(TRIM(language_raw),''))=1
             THEN MAX(NULLIF(TRIM(language_raw),'')) END AS resolved_language,
        CASE WHEN COUNT(DISTINCT NULLIF(TRIM(osname_raw),''))=1
             THEN MAX(NULLIF(TRIM(osname_raw),'')) END AS resolved_osname,
        CASE WHEN COUNT(DISTINCT NULLIF(TRIM(osversion_raw),''))=1
             THEN MAX(NULLIF(TRIM(osversion_raw),'')) END AS resolved_osversion,
        CASE WHEN COUNT(DISTINCT NULLIF(TRIM(versionname_raw),''))=1
             THEN MAX(NULLIF(TRIM(versionname_raw),'')) END AS resolved_versionname,
        CASE WHEN COUNT(DISTINCT normalized_device_id)=1
             THEN MAX(normalized_device_id) END AS resolved_device_id
    FROM votes_mart.bridge_hackle_session_property_raw_v2
    /* NULL/blank session은 안정적인 연결키가 아니므로 사용자·기기에 귀속하지 않는다.
       그 외에는 TRIM 값이 아닌 원문 session_id로 묶어 서로 다른 원문을 합치지 않는다. */
    WHERE NULLIF(TRIM(original_session_id_raw),'') IS NOT NULL
    GROUP BY original_session_id_raw
)
SELECT
    sk.session_sk,sk.original_session_id,
    sk.normalized_session_id AS session_id,sk.normalized_session_id,
    sk.original_session_id_state,
    COALESCE(pr.property_row_count,0) AS property_row_count,
    COALESCE(pr.distinct_raw_user_id_count,0) AS distinct_raw_user_id_count,
    COALESCE(pr.distinct_language_count,0) AS distinct_language_count,
    COALESCE(pr.distinct_osname_count,0) AS distinct_osname_count,
    COALESCE(pr.distinct_osversion_count,0) AS distinct_osversion_count,
    COALESCE(pr.distinct_versionname_count,0) AS distinct_versionname_count,
    COALESCE(pr.distinct_device_id_count,0) AS distinct_device_id_count,
    pr.resolved_raw_user_id,u.hackle_user_sk AS resolved_hackle_user_sk,
    u.service_user_id_candidate AS resolved_account_user_id_candidate,
    CASE
        WHEN pr.original_session_id_raw IS NULL THEN 'NO_SESSION_PROPERTY'
        WHEN pr.distinct_raw_user_id_count=0 THEN 'MISSING'
        WHEN pr.distinct_raw_user_id_count>1 THEN 'AMBIGUOUS_MULTIPLE_USER_IDS'
        WHEN pr.resolved_raw_user_id REGEXP '^[0-9]+$' THEN 'UNIQUE_NUMERIC'
        ELSE 'UNIQUE_NONNUMERIC'
    END AS user_resolution_status,
    (COALESCE(pr.distinct_raw_user_id_count,0)>1) AS user_conflict_flag,
    pr.resolved_language,pr.resolved_osname,pr.resolved_osversion,
    pr.resolved_versionname,
    (COALESCE(pr.distinct_language_count,0)>1) AS language_conflict_flag,
    (COALESCE(pr.distinct_osname_count,0)>1) AS osname_conflict_flag,
    (COALESCE(pr.distinct_osversion_count,0)>1) AS osversion_conflict_flag,
    (COALESCE(pr.distinct_versionname_count,0)>1) AS versionname_conflict_flag,
    pr.resolved_device_id,d.device_sk AS resolved_device_sk,
    (COALESCE(pr.distinct_device_id_count,0)>1) AS device_conflict_flag
FROM session_keyed AS sk
LEFT JOIN property_rollup AS pr
  ON pr.original_session_id_raw=sk.original_session_id
LEFT JOIN votes_mart.dim_hackle_user_resolved_v2 AS u
  ON u.normalized_raw_user_id=pr.resolved_raw_user_id
 AND pr.distinct_raw_user_id_count=1
LEFT JOIN votes_mart.dim_hackle_device_resolved_v2 AS d
  ON d.normalized_device_id=pr.resolved_device_id
 AND pr.distinct_device_id_count=1;

ALTER TABLE votes_mart.dim_hackle_session_resolved_v2
    ADD PRIMARY KEY (session_sk),
    ADD KEY idx_hsr_original_session (original_session_id),
    ADD KEY idx_hsr_normalized_session (normalized_session_id),
    ADD KEY idx_hsr_user (resolved_hackle_user_sk),
    ADD KEY idx_hsr_device (resolved_device_sk);


/* --------------------------------------------------------------------------
   6. 좁은 event fact. 원시 문자열은 surrogate key로 저장한다.
---------------------------------------------------------------------------- */
CREATE TABLE votes_mart.fact_hackle_event_24d_v2 AS
WITH numbered AS (
    SELECT ROW_NUMBER() OVER(ORDER BY he.event_id) AS event_sk,he.*
    FROM final.hackle_events AS he
)
SELECT
    n.event_sk,n.event_id,n.event_datetime AS event_datetime_raw,
    DATE(n.event_datetime) AS event_date_raw,
    s.session_sk AS original_session_sk,
    ek.text_attribute_sk AS event_key_attribute_sk,
    rid.text_attribute_sk AS raw_id_attribute_sk,
    item.text_attribute_sk AS item_name_attribute_sk,
    page.text_attribute_sk AS page_name_attribute_sk,
    n.friend_count,n.votes_count,n.heart_balance,n.question_id
FROM numbered AS n
JOIN votes_mart.dim_hackle_session_resolved_v2 AS s
  ON s.original_session_id <=> n.session_id
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS ek
  ON ek.attribute_type='EVENT_KEY' AND ek.attribute_value_raw <=> n.event_key
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS rid
  ON rid.attribute_type='RAW_ID_ATTRIBUTE' AND rid.attribute_value_raw <=> n.id
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS item
  ON item.attribute_type='ITEM_NAME' AND item.attribute_value_raw <=> n.item_name
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS page
  ON page.attribute_type='PAGE_NAME' AND page.attribute_value_raw <=> n.page_name;

ALTER TABLE votes_mart.fact_hackle_event_24d_v2
    ADD PRIMARY KEY (event_sk),
    ADD UNIQUE KEY uk_hackle_fact_event_id (event_id),
    ADD KEY idx_hackle_fact_time (event_datetime_raw),
    ADD KEY idx_hackle_fact_event_key (event_key_attribute_sk),
    ADD KEY idx_hackle_fact_date_key (event_date_raw,event_key_attribute_sk),
    ADD KEY idx_hackle_fact_session_time (original_session_sk,event_datetime_raw),
    ADD KEY idx_hackle_fact_question (question_id);


/* --------------------------------------------------------------------------
   7. event -> 15/30/60분 방문 배정. 기본 30분 ID만 BINARY(16) 저장.
   15/60분 방문키는 각각
   (session_partition_sk, visit_sequence_15m),
   (session_partition_sk, visit_sequence_60m) 복합키다.
---------------------------------------------------------------------------- */
CREATE TABLE votes_mart.bridge_hackle_event_visit_assignment_v2 AS
WITH ordered AS (
    SELECT
        f.event_sk,f.original_session_sk,f.event_datetime_raw,
        CASE WHEN s.original_session_id_state='VALUE'
             THEN CAST(s.session_sk AS SIGNED)
             ELSE -CAST(f.event_sk AS SIGNED) END AS session_partition_sk,
        ROW_NUMBER() OVER(
            PARTITION BY CASE WHEN s.original_session_id_state='VALUE'
                              THEN CAST(s.session_sk AS SIGNED)
                              ELSE -CAST(f.event_sk AS SIGNED) END
            ORDER BY (f.event_datetime_raw IS NULL),f.event_datetime_raw,f.event_sk
        ) AS event_order_in_original_session,
        LAG(f.event_datetime_raw) OVER(
            PARTITION BY CASE WHEN s.original_session_id_state='VALUE'
                              THEN CAST(s.session_sk AS SIGNED)
                              ELSE -CAST(f.event_sk AS SIGNED) END
            ORDER BY (f.event_datetime_raw IS NULL),f.event_datetime_raw,f.event_sk
        ) AS previous_event_at_in_original_session
    FROM votes_mart.fact_hackle_event_24d_v2 AS f
    JOIN votes_mart.dim_hackle_session_resolved_v2 AS s
      ON s.session_sk=f.original_session_sk
),
broken AS (
    SELECT
        o.*,
        TIMESTAMPDIFF(SECOND,o.previous_event_at_in_original_session,o.event_datetime_raw)
            AS seconds_since_previous_event,
        CASE WHEN o.event_order_in_original_session=1 OR o.event_datetime_raw IS NULL
                  OR TIMESTAMPDIFF(SECOND,o.previous_event_at_in_original_session,
                                      o.event_datetime_raw)>=15*60
             THEN 1 ELSE 0 END AS is_break_15m,
        CASE WHEN o.event_order_in_original_session=1 OR o.event_datetime_raw IS NULL
                  OR TIMESTAMPDIFF(SECOND,o.previous_event_at_in_original_session,
                                      o.event_datetime_raw)>=30*60
             THEN 1 ELSE 0 END AS is_break_30m,
        CASE WHEN o.event_order_in_original_session=1 OR o.event_datetime_raw IS NULL
                  OR TIMESTAMPDIFF(SECOND,o.previous_event_at_in_original_session,
                                      o.event_datetime_raw)>=60*60
             THEN 1 ELSE 0 END AS is_break_60m
    FROM ordered AS o
),
sequenced AS (
    SELECT
        b.*,
        SUM(is_break_15m) OVER(
            PARTITION BY session_partition_sk
            ORDER BY (event_datetime_raw IS NULL),event_datetime_raw,event_sk
            ROWS UNBOUNDED PRECEDING) AS visit_sequence_15m,
        SUM(is_break_30m) OVER(
            PARTITION BY session_partition_sk
            ORDER BY (event_datetime_raw IS NULL),event_datetime_raw,event_sk
            ROWS UNBOUNDED PRECEDING) AS visit_sequence_30m,
        SUM(is_break_60m) OVER(
            PARTITION BY session_partition_sk
            ORDER BY (event_datetime_raw IS NULL),event_datetime_raw,event_sk
            ROWS UNBOUNDED PRECEDING) AS visit_sequence_60m
    FROM broken AS b
),
identified AS (
    SELECT
        s.*,
        UNHEX(MD5(CONCAT(CAST(session_partition_sk AS CHAR),'|30m|',
                         CAST(visit_sequence_30m AS CHAR)))) AS derived_visit_id_30m
    FROM sequenced AS s
)
SELECT
    i.event_sk,i.original_session_sk,i.session_partition_sk,
    i.event_order_in_original_session,i.previous_event_at_in_original_session,
    i.seconds_since_previous_event,i.is_break_15m,i.is_break_30m,i.is_break_60m,
    i.visit_sequence_15m,i.visit_sequence_30m,i.visit_sequence_60m,
    i.derived_visit_id_30m,
    ROW_NUMBER() OVER(
        PARTITION BY i.derived_visit_id_30m
        ORDER BY (i.event_datetime_raw IS NULL),i.event_datetime_raw,i.event_sk
    ) AS event_order_in_visit_30m
FROM identified AS i;

ALTER TABLE votes_mart.bridge_hackle_event_visit_assignment_v2
    ADD PRIMARY KEY (event_sk),
    ADD KEY idx_heva_visit30 (derived_visit_id_30m,event_order_in_visit_30m),
    ADD KEY idx_heva_partition15 (session_partition_sk,visit_sequence_15m),
    ADD KEY idx_heva_partition30 (session_partition_sk,visit_sequence_30m),
    ADD KEY idx_heva_partition60 (session_partition_sk,visit_sequence_60m),
    ADD KEY idx_heva_session (original_session_sk);


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


