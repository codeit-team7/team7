/* ============================================================================
   01. 사용자 가입 맥락 + 학교 인접 관계 + 연락처 초대자 관계

   원천 커버리지
   - final.accounts_user
   - final.accounts_group
   - final.accounts_school
   - final.accounts_user_contacts
   - final.accounts_nearbyschool

   기존 테이블을 덮어쓰지 않고 _v2만 재생성한다.
============================================================================ */

CREATE DATABASE IF NOT EXISTS votes_mart;

/* 학교·학급 원천에 존재하지만 현재 사용자가 0명인 행도 잃지 않는 기준표 */
DROP TABLE IF EXISTS votes_mart.dim_school_current_v2;

CREATE TABLE votes_mart.dim_school_current_v2 AS
SELECT
    s.id AS school_id,
    s.address AS school_address,
    s.student_count AS school_student_count_source,
    s.school_type,
    'CURRENT_SCHOOL_SNAPSHOT_UNKNOWN_ASOF' AS dimension_time_scope
FROM final.accounts_school AS s;

ALTER TABLE votes_mart.dim_school_current_v2
    ADD PRIMARY KEY (school_id);

DROP TABLE IF EXISTS votes_mart.dim_group_current_v2;

CREATE TABLE votes_mart.dim_group_current_v2 AS
SELECT
    g.id AS group_id,
    g.grade,
    g.class_num,
    g.school_id,
    (s.school_id IS NULL) AS orphan_school_flag,
    'CURRENT_GROUP_SNAPSHOT_UNKNOWN_ASOF' AS dimension_time_scope
FROM final.accounts_group AS g
LEFT JOIN votes_mart.dim_school_current_v2 AS s
  ON s.school_id = g.school_id;

ALTER TABLE votes_mart.dim_group_current_v2
    ADD PRIMARY KEY (group_id),
    ADD INDEX idx_group_school (school_id);

/* contacts 원행 1건을 먼저 전부 보존하고, 집계·JSON 전개는 이 원장에서 한다. */
DROP TABLE IF EXISTS votes_mart.mart_user_contact_record_v2;

CREATE TABLE votes_mart.mart_user_contact_record_v2 AS
SELECT
    c.id AS contact_record_id,
    c.user_id,
    c.contacts_count AS contacts_count_source,
    c.invite_user_id_list AS invite_user_id_list_json,
    JSON_VALID(c.invite_user_id_list) AS invite_list_json_valid,
    CASE WHEN JSON_VALID(c.invite_user_id_list)=1
         THEN JSON_LENGTH(c.invite_user_id_list) END AS invite_user_id_list_length,
    (u.id IS NULL) AS orphan_user_flag,
    'CONTACT_SNAPSHOT_TIME_NOT_AVAILABLE' AS contact_time_scope
FROM final.accounts_user_contacts AS c
LEFT JOIN final.accounts_user AS u
  ON u.id = c.user_id;

ALTER TABLE votes_mart.mart_user_contact_record_v2
    ADD PRIMARY KEY (contact_record_id),
    ADD INDEX idx_contact_record_user (user_id);

DROP TABLE IF EXISTS votes_mart.mart_user_acquisition_profile_v2;

CREATE TABLE votes_mart.mart_user_acquisition_profile_v2 AS
WITH user_current AS (
    SELECT
        u.id AS user_id,
        u.is_superuser,
        u.is_staff,
        u.gender,
        u.point AS current_point_snapshot,
        u.friend_id_list AS current_friend_id_list_json,
        u.is_push_on,
        u.created_at AS signup_at,
        u.block_user_id_list AS current_block_user_id_list_json,
        u.hide_user_id_list AS current_hide_user_id_list_json,
        u.ban_status AS current_ban_status,
        u.report_count AS current_report_count,
        u.alarm_count AS current_alarm_count,
        u.pending_chat AS current_pending_chat,
        u.pending_votes AS current_pending_votes,
        u.group_id AS current_group_id,
        g.grade AS current_grade,
        g.class_num AS current_class_num,
        g.school_id AS current_school_id,
        s.address AS current_school_address,
        s.student_count AS current_school_student_count_source,
        s.school_type AS current_school_type,
        JSON_VALID(u.friend_id_list) AS current_friend_json_valid,
        CASE WHEN JSON_VALID(u.friend_id_list)=1
             THEN JSON_LENGTH(u.friend_id_list) END AS current_friend_list_length,
        JSON_VALID(u.block_user_id_list) AS current_block_json_valid,
        CASE WHEN JSON_VALID(u.block_user_id_list)=1
             THEN JSON_LENGTH(u.block_user_id_list) END AS current_block_list_length,
        JSON_VALID(u.hide_user_id_list) AS current_hide_json_valid,
        CASE WHEN JSON_VALID(u.hide_user_id_list)=1
             THEN JSON_LENGTH(u.hide_user_id_list) END AS current_hide_list_length,
        ROW_NUMBER() OVER (
            PARTITION BY g.school_id
            ORDER BY u.created_at, u.id
        ) AS current_roster_school_signup_rank
    FROM final.accounts_user AS u
    LEFT JOIN final.accounts_group AS g
      ON g.id = u.group_id
    LEFT JOIN final.accounts_school AS s
      ON s.id = g.school_id
),
school_signup_context AS (
    SELECT
        current_school_id AS school_id,
        COUNT(*) AS current_roster_account_count,
        MIN(signup_at) AS current_roster_first_signup_at,
        MIN(CASE WHEN current_roster_school_signup_rank=40
                 THEN signup_at END) AS current_roster_40th_signup_at,
        MAX(signup_at) AS current_roster_last_signup_at
    FROM user_current
    WHERE current_school_id IS NOT NULL
    GROUP BY current_school_id
),
contacts_one_row AS (
    SELECT
        c.user_id,
        COUNT(*) AS contact_source_row_count,
        MIN(c.contact_record_id) AS contact_record_id,
        MAX(c.contacts_count_source) AS contacts_count_source,
        MAX(c.invite_user_id_list_json) AS invite_user_id_list_json,
        MAX(c.invite_list_json_valid) AS invite_list_json_valid,
        MAX(c.invite_user_id_list_length) AS invite_user_id_list_length
    FROM votes_mart.mart_user_contact_record_v2 AS c
    GROUP BY c.user_id
),
nearby_summary AS (
    SELECT
        n.school_id,
        COUNT(*) AS nearby_relation_row_count,
        COUNT(DISTINCT n.nearby_school_id) AS nearby_school_count,
        MIN(n.distance) AS nearby_distance_min_raw,
        AVG(n.distance) AS nearby_distance_avg_raw,
        MAX(n.distance) AS nearby_distance_max_raw,
        SUM(n.school_id=n.nearby_school_id) AS nearby_self_relation_count,
        SUM(n.distance=0) AS nearby_zero_distance_count
    FROM final.accounts_nearbyschool AS n
    GROUP BY n.school_id
)
SELECT
    uc.*,
    sc.current_roster_account_count,
    sc.current_roster_first_signup_at,
    sc.current_roster_40th_signup_at,
    sc.current_roster_last_signup_at,
    (sc.current_roster_40th_signup_at IS NOT NULL) AS current_roster_reached_40_flag,
    TIMESTAMPDIFF(
        SECOND,
        sc.current_roster_first_signup_at,
        sc.current_roster_40th_signup_at
    ) AS seconds_first_to_40th_current_roster,
    co.contact_source_row_count,
    co.contact_record_id,
    (co.user_id IS NOT NULL) AS contacts_observed_flag,
    co.contacts_count_source,
    co.invite_user_id_list_json,
    co.invite_list_json_valid,
    co.invite_user_id_list_length,
    ns.nearby_relation_row_count,
    ns.nearby_school_count,
    ns.nearby_distance_min_raw,
    ns.nearby_distance_avg_raw,
    ns.nearby_distance_max_raw,
    ns.nearby_self_relation_count,
    ns.nearby_zero_distance_count,
    'CURRENT_PROFILE_UNKNOWN_ASOF' AS profile_time_scope
FROM user_current AS uc
LEFT JOIN school_signup_context AS sc
  ON sc.school_id = uc.current_school_id
LEFT JOIN contacts_one_row AS co
  ON co.user_id = uc.user_id
LEFT JOIN nearby_summary AS ns
  ON ns.school_id = uc.current_school_id;

ALTER TABLE votes_mart.mart_user_acquisition_profile_v2
    ADD PRIMARY KEY (user_id),
    ADD INDEX idx_acq_school_signup (current_school_id, signup_at),
    ADD INDEX idx_acq_group (current_group_id),
    ADD INDEX idx_acq_contact (contacts_observed_flag);


/*
   friend_id_list에는 약 3,610만 개 참조가 있다. 이를 물리 테이블로 복제하면
   원천보다 큰 디스크와 여러 인덱스가 필요하므로, 원본 JSON을 필요할 때만
   펼치는 VIEW로 제공한다. 즉 모든 원소와 순번은 그대로 접근할 수 있지만
   전체 edge가 상시 디스크를 차지하지 않는다.

   반복 실행 호환성:
   - 예전 번들이 만든 물리 테이블이 있으면 먼저 제거한다.
   - 이미 VIEW인 경우 DROP TABLE IF EXISTS는 해당 VIEW를 제거하지 않고,
     아래 CREATE OR REPLACE VIEW가 정의만 교체한다.
*/
SET @drop_current_friend_edge_sql = (
    SELECT CASE TABLE_TYPE
        WHEN 'VIEW' THEN 'DROP VIEW votes_mart.bridge_current_friend_edge_v2'
        WHEN 'BASE TABLE' THEN 'DROP TABLE votes_mart.bridge_current_friend_edge_v2'
        ELSE 'DO 0'
    END
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA='votes_mart'
      AND TABLE_NAME='bridge_current_friend_edge_v2'
    LIMIT 1
);
SET @drop_current_friend_edge_sql=COALESCE(@drop_current_friend_edge_sql,'DO 0');
PREPARE stmt_drop_current_friend_edge FROM @drop_current_friend_edge_sql;
EXECUTE stmt_drop_current_friend_edge;
DEALLOCATE PREPARE stmt_drop_current_friend_edge;

CREATE OR REPLACE VIEW votes_mart.bridge_current_friend_edge_v2 AS
WITH expanded AS (
    SELECT
        p.user_id AS source_user_id,
        jt.friend_ordinal,
        jt.target_user_id,
        ROW_NUMBER() OVER (
            PARTITION BY p.user_id, jt.target_user_id
            ORDER BY jt.friend_ordinal
        ) AS duplicate_reference_rank
    FROM votes_mart.mart_user_acquisition_profile_v2 AS p
    JOIN JSON_TABLE(
        CASE WHEN p.current_friend_json_valid=1
             THEN p.current_friend_id_list_json ELSE JSON_ARRAY() END,
        '$[*]' COLUMNS (
            friend_ordinal FOR ORDINALITY,
            target_user_id BIGINT PATH '$'
        )
    ) AS jt ON TRUE
)
SELECT
    e.source_user_id,
    e.friend_ordinal,
    e.target_user_id,
    e.duplicate_reference_rank,
    (e.duplicate_reference_rank>1) AS duplicate_reference_flag,
    (e.source_user_id=e.target_user_id) AS self_reference_flag,
    (target.user_id IS NULL) AS orphan_target_flag,
    source.current_school_id AS source_current_school_id,
    source.current_grade AS source_current_grade,
    source.current_class_num AS source_current_class_num,
    target.current_school_id AS target_current_school_id,
    target.current_grade AS target_current_grade,
    target.current_class_num AS target_current_class_num,
    (source.current_school_id=target.current_school_id) AS same_school_current_flag,
    (source.current_school_id=target.current_school_id
     AND source.current_grade=target.current_grade) AS same_school_grade_current_flag,
    (source.current_group_id=target.current_group_id) AS same_group_current_flag,
    'CURRENT_FRIEND_SNAPSHOT_UNKNOWN_ASOF' AS relationship_time_scope
FROM expanded AS e
JOIN votes_mart.mart_user_acquisition_profile_v2 AS source
  ON source.user_id=e.source_user_id
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS target
  ON target.user_id=e.target_user_id;


DROP TABLE IF EXISTS votes_mart.bridge_school_neighbor_v2;

CREATE TABLE votes_mart.bridge_school_neighbor_v2 AS
SELECT
    n.id AS nearby_relation_id,
    n.school_id,
    n.nearby_school_id,
    n.distance AS distance_raw_unit_unknown,
    (n.school_id=n.nearby_school_id) AS self_school_relation_flag,
    (n.distance=0) AS zero_distance_flag,
    s1.school_type AS school_type,
    s1.address AS school_address,
    s1.student_count AS school_student_count_source,
    s2.school_type AS nearby_school_type,
    s2.address AS nearby_school_address,
    s2.student_count AS nearby_school_student_count_source,
    (s1.id IS NULL) AS orphan_school_flag,
    (s2.id IS NULL) AS orphan_nearby_school_flag,
    p1.current_roster_account_count AS school_current_roster_account_count,
    p1.current_roster_first_signup_at AS school_first_signup_at,
    p1.current_roster_40th_signup_at AS school_40th_signup_at,
    p2.current_roster_account_count AS nearby_current_roster_account_count,
    p2.current_roster_first_signup_at AS nearby_first_signup_at,
    p2.current_roster_40th_signup_at AS nearby_40th_signup_at,
    TIMESTAMPDIFF(
        SECOND,
        p1.current_roster_first_signup_at,
        p2.current_roster_first_signup_at
    ) AS seconds_between_first_signups
FROM final.accounts_nearbyschool AS n
LEFT JOIN final.accounts_school AS s1
  ON s1.id = n.school_id
LEFT JOIN final.accounts_school AS s2
  ON s2.id = n.nearby_school_id
LEFT JOIN (
    SELECT DISTINCT
        current_school_id,
        current_roster_account_count,
        current_roster_first_signup_at,
        current_roster_40th_signup_at
    FROM votes_mart.mart_user_acquisition_profile_v2
    WHERE current_school_id IS NOT NULL
) AS p1
  ON p1.current_school_id = n.school_id
LEFT JOIN (
    SELECT DISTINCT
        current_school_id,
        current_roster_account_count,
        current_roster_first_signup_at,
        current_roster_40th_signup_at
    FROM votes_mart.mart_user_acquisition_profile_v2
    WHERE current_school_id IS NOT NULL
) AS p2
  ON p2.current_school_id = n.nearby_school_id;

ALTER TABLE votes_mart.bridge_school_neighbor_v2
    ADD PRIMARY KEY (nearby_relation_id),
    ADD INDEX idx_neighbor_pair (school_id, nearby_school_id),
    ADD INDEX idx_neighbor_reverse (nearby_school_id, school_id);


DROP TABLE IF EXISTS votes_mart.bridge_contact_inviter_v2;

CREATE TABLE votes_mart.bridge_contact_inviter_v2 AS
WITH expanded AS (
    SELECT
        c.contact_record_id,
        c.user_id AS contact_owner_user_id,
        c.contacts_count_source,
        jt.invite_ordinal,
        jt.inviter_user_id,
        ROW_NUMBER() OVER (
            PARTITION BY c.contact_record_id, jt.inviter_user_id
            ORDER BY jt.invite_ordinal
        ) AS same_inviter_duplicate_rank
    FROM votes_mart.mart_user_contact_record_v2 AS c
    JOIN JSON_TABLE(
        CASE WHEN c.invite_list_json_valid=1
             THEN c.invite_user_id_list_json ELSE JSON_ARRAY() END,
        '$[*]' COLUMNS (
            invite_ordinal FOR ORDINALITY,
            inviter_user_id BIGINT PATH '$'
        )
    ) AS jt ON TRUE
)
SELECT
    e.*,
    (e.same_inviter_duplicate_rank>1) AS duplicate_inviter_reference_flag,
    owner.current_school_id AS contact_owner_current_school_id,
    owner.current_grade AS contact_owner_current_grade,
    owner.current_class_num AS contact_owner_current_class_num,
    inviter.current_school_id AS inviter_current_school_id,
    inviter.current_grade AS inviter_current_grade,
    inviter.current_class_num AS inviter_current_class_num,
    (owner.user_id IS NULL) AS orphan_contact_owner_flag,
    (inviter.user_id IS NULL) AS orphan_inviter_flag,
    (owner.current_school_id=inviter.current_school_id) AS same_school_current_flag,
    (owner.current_school_id=inviter.current_school_id
     AND owner.current_grade=inviter.current_grade) AS same_school_grade_current_flag,
    (owner.current_group_id=inviter.current_group_id) AS same_group_current_flag,
    'CURRENT_PROFILE_UNKNOWN_ASOF' AS profile_time_scope
FROM expanded AS e
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS owner
  ON owner.user_id = e.contact_owner_user_id
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS inviter
  ON inviter.user_id = e.inviter_user_id;

ALTER TABLE votes_mart.bridge_contact_inviter_v2
    ADD INDEX idx_contact_record (contact_record_id),
    ADD INDEX idx_contact_owner (contact_owner_user_id),
    ADD INDEX idx_contact_inviter (inviter_user_id);
