/* ============================================================================
   02. 친구요청·학교 확산 v2 — 디스크 안전형

   설계 원칙
   1) accounts_friendrequest 17,147,175행은 원행 grain을 100% 보존한다.
   2) 대형 사실표에는 원천 6개 필드와 재사용 빈도가 높은 좁은 파생키만 둔다.
   3) 사용자·학교·친구 JSON·연락처·인근학교 속성은 사실표에 1,714만 번
      복제하지 않는다. 필요할 때 profile/dim/bridge 또는 아래 VIEW로 붙인다.
   4) status는 최종 스냅샷이지 상태 변경 이력이 아니다.
   5) 현재 학교·친구 관계는 기준시점이 알려지지 않은 현재 스냅샷이다.

   후속 호환 컬럼
   - 03: send_user_id, receive_user_id, final_status_code,
         request_created_at_raw, request_updated_at_raw
   - 06: 위 컬럼 + request_created_date_raw, request_updated_date_raw
   - 07: request_id
============================================================================ */

CREATE DATABASE IF NOT EXISTS votes_mart;


/* ============================================================================
   1. 친구요청 원행 팩트 — request_id 1행, 좁은 물리 테이블
============================================================================ */

DROP TABLE IF EXISTS votes_mart.mart_friend_request_event_v2;

CREATE TABLE votes_mart.mart_friend_request_event_v2 AS
SELECT
    fr.id AS request_id,
    fr.send_user_id,
    fr.receive_user_id,
    fr.status AS final_status_code,
    fr.created_at AS request_created_at_raw,
    DATE(fr.created_at) AS request_created_date_raw,
    fr.updated_at AS request_updated_at_raw,
    DATE(fr.updated_at) AS request_updated_date_raw,
    CASE
        WHEN fr.updated_at IS NULL OR fr.created_at IS NULL THEN NULL
        ELSE TIMESTAMPDIFF(SECOND, fr.created_at, fr.updated_at)
    END AS created_to_last_update_seconds,
    CASE
        WHEN fr.updated_at IS NOT NULL
         AND fr.created_at IS NOT NULL
         AND fr.updated_at < fr.created_at THEN 1 ELSE 0
    END AS negative_update_interval_flag,
    (fr.send_user_id = fr.receive_user_id) AS self_request_flag
FROM final.accounts_friendrequest AS fr;

ALTER TABLE votes_mart.mart_friend_request_event_v2
    ADD PRIMARY KEY (request_id),
    ADD INDEX idx_fr_created (request_created_at_raw, final_status_code),
    ADD INDEX idx_fr_updated (request_updated_at_raw, final_status_code),
    ADD INDEX idx_fr_sender_created (send_user_id, request_created_at_raw),
    ADD INDEX idx_fr_receiver_created (receive_user_id, request_created_at_raw),
    ADD INDEX idx_fr_pair (send_user_id, receive_user_id);


/* 동일 방향 사용자쌍 반복은 이벤트 fact에 window 결과를 1,714만 번 복제하지 않고
   pair grain의 별도 좁은 요약으로 보존한다. */
DROP TABLE IF EXISTS votes_mart.mart_friend_request_pair_summary_v2;

CREATE TABLE votes_mart.mart_friend_request_pair_summary_v2 AS
SELECT
    send_user_id,
    receive_user_id,
    COUNT(*) AS request_record_count,
    SUM(final_status_code='A') AS final_accepted_record_count,
    SUM(final_status_code='P') AS final_pending_record_count,
    SUM(final_status_code='R') AS final_rejected_record_count,
    MIN(request_created_at_raw) AS first_request_created_at_raw,
    MAX(request_created_at_raw) AS last_request_created_at_raw,
    MIN(CASE WHEN final_status_code='A'
             THEN COALESCE(request_updated_at_raw,request_created_at_raw) END)
        AS first_accepted_effective_at_proxy,
    MAX(CASE WHEN final_status_code='A'
             THEN COALESCE(request_updated_at_raw,request_created_at_raw) END)
        AS last_accepted_effective_at_proxy,
    MIN(request_updated_at_raw) AS first_last_update_at_raw,
    MAX(request_updated_at_raw) AS last_last_update_at_raw
FROM votes_mart.mart_friend_request_event_v2
GROUP BY send_user_id, receive_user_id;

ALTER TABLE votes_mart.mart_friend_request_pair_summary_v2
    ADD PRIMARY KEY (send_user_id, receive_user_id),
    ADD INDEX idx_fr_pair_summary_receiver (receive_user_id, send_user_id);


/* ============================================================================
   1-A. 현재 사용자·학교 문맥 VIEW

   물리 fact를 넓히지 않고 요청 시점에 현재 profile을 붙여 보는 편의 VIEW다.
   현재 친구 포함 여부는 bridge_current_friend_edge_v2를 필요한 사용자쌍에만
   조인하고, 연락처/인근학교도 각각 bridge_contact_inviter_v2와
   bridge_school_neighbor_v2를 필요한 쌍에만 조인한다.
============================================================================ */

CREATE OR REPLACE VIEW votes_mart.vw_friend_request_event_current_context_v2 AS
SELECT
    f.*,
    sender.signup_at AS sender_signup_at,
    sender.gender AS sender_gender_current,
    sender.is_staff AS sender_is_staff_current,
    sender.is_superuser AS sender_is_superuser_current,
    sender.current_group_id AS sender_group_id_current,
    sender.current_school_id AS sender_school_id_current,
    sender.current_school_type AS sender_school_type_current,
    sender.current_grade AS sender_grade_current,
    sender.current_class_num AS sender_class_num_current,
    receiver.signup_at AS receiver_signup_at,
    receiver.gender AS receiver_gender_current,
    receiver.is_staff AS receiver_is_staff_current,
    receiver.is_superuser AS receiver_is_superuser_current,
    receiver.current_group_id AS receiver_group_id_current,
    receiver.current_school_id AS receiver_school_id_current,
    receiver.current_school_type AS receiver_school_type_current,
    receiver.current_grade AS receiver_grade_current,
    receiver.current_class_num AS receiver_class_num_current,
    (sender.user_id IS NULL) AS orphan_sender_user_flag,
    (receiver.user_id IS NULL) AS orphan_receiver_user_flag,
    CASE
        WHEN sender.signup_at IS NULL OR f.request_created_at_raw IS NULL THEN NULL
        ELSE TIMESTAMPDIFF(SECOND, sender.signup_at, f.request_created_at_raw)
    END AS sender_signup_to_request_seconds,
    CASE
        WHEN receiver.signup_at IS NULL OR f.request_created_at_raw IS NULL THEN NULL
        ELSE TIMESTAMPDIFF(SECOND, receiver.signup_at, f.request_created_at_raw)
    END AS receiver_signup_to_request_seconds,
    CASE
        WHEN sender.current_school_id IS NULL OR receiver.current_school_id IS NULL
            THEN NULL
        ELSE sender.current_school_id = receiver.current_school_id
    END AS same_school_current_flag,
    CASE
        WHEN sender.current_school_id IS NULL OR receiver.current_school_id IS NULL
          OR sender.current_grade IS NULL OR receiver.current_grade IS NULL THEN NULL
        ELSE sender.current_school_id = receiver.current_school_id
         AND sender.current_grade = receiver.current_grade
    END AS same_school_grade_current_flag,
    CASE
        WHEN sender.current_group_id IS NULL OR receiver.current_group_id IS NULL
            THEN NULL
        ELSE sender.current_group_id = receiver.current_group_id
    END AS same_group_current_flag
FROM votes_mart.mart_friend_request_event_v2 AS f
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS sender
  ON sender.user_id = f.send_user_id
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS receiver
  ON receiver.user_id = f.receive_user_id;


/* ============================================================================
   2. 사용자별 가입·친구요청 요약 — user_id 1행

   요청이 없는 사용자도 보존한다. acquisition profile의 대형 JSON 원문은
   다시 복제하지 않고, 원문은 mart_user_acquisition_profile_v2에서 조회한다.
============================================================================ */

DROP TABLE IF EXISTS votes_mart.mart_user_viral_profile_v2;

CREATE TABLE votes_mart.mart_user_viral_profile_v2 AS
WITH sent_summary AS (
    SELECT
        f.send_user_id AS user_id,
        COUNT(*) AS sent_request_count,
        COUNT(DISTINCT f.receive_user_id) AS sent_unique_partner_count,
        SUM(f.final_status_code = 'A') AS sent_final_accepted_count,
        SUM(f.final_status_code = 'P') AS sent_final_pending_count,
        SUM(f.final_status_code = 'R') AS sent_final_rejected_count,
        COUNT(*)-COUNT(DISTINCT f.receive_user_id) AS sent_repeated_request_count,
        SUM(TIMESTAMPDIFF(SECOND, p.signup_at, f.request_created_at_raw)
            BETWEEN 0 AND 86399) AS sent_request_first_24h_count,
        SUM(TIMESTAMPDIFF(SECOND, p.signup_at, f.request_created_at_raw)
            BETWEEN 0 AND 259199) AS sent_request_first_72h_count,
        SUM(TIMESTAMPDIFF(SECOND, p.signup_at, f.request_created_at_raw)
            BETWEEN 0 AND 604799) AS sent_request_first_7d_count,
        MIN(f.request_created_at_raw) AS first_sent_request_at_raw,
        MAX(f.request_created_at_raw) AS last_sent_request_at_raw
    FROM votes_mart.mart_friend_request_event_v2 AS f
    LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS p
      ON p.user_id = f.send_user_id
    GROUP BY f.send_user_id
),
received_summary AS (
    SELECT
        f.receive_user_id AS user_id,
        COUNT(*) AS received_request_count,
        COUNT(DISTINCT f.send_user_id) AS received_unique_partner_count,
        SUM(f.final_status_code = 'A') AS received_final_accepted_count,
        SUM(f.final_status_code = 'P') AS received_final_pending_count,
        SUM(f.final_status_code = 'R') AS received_final_rejected_count,
        SUM(TIMESTAMPDIFF(SECOND, p.signup_at, f.request_created_at_raw)
            BETWEEN 0 AND 86399) AS received_request_first_24h_count,
        SUM(TIMESTAMPDIFF(SECOND, p.signup_at, f.request_created_at_raw)
            BETWEEN 0 AND 259199) AS received_request_first_72h_count,
        SUM(TIMESTAMPDIFF(SECOND, p.signup_at, f.request_created_at_raw)
            BETWEEN 0 AND 604799) AS received_request_first_7d_count,
        MIN(f.request_created_at_raw) AS first_received_request_at_raw,
        MAX(f.request_created_at_raw) AS last_received_request_at_raw
    FROM votes_mart.mart_friend_request_event_v2 AS f
    LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS p
      ON p.user_id = f.receive_user_id
    GROUP BY f.receive_user_id
)
SELECT
    p.user_id,
    p.signup_at,
    p.gender,
    p.is_staff,
    p.is_superuser,
    p.current_group_id,
    p.current_school_id,
    p.current_school_type,
    p.current_grade,
    p.current_class_num,
    p.current_friend_list_length,
    p.contacts_observed_flag,
    p.contacts_count_source,
    p.invite_user_id_list_length,
    p.current_roster_first_signup_at,
    p.current_roster_40th_signup_at,
    COALESCE(ss.sent_request_count, 0) AS sent_request_count,
    COALESCE(ss.sent_unique_partner_count, 0) AS sent_unique_partner_count,
    COALESCE(ss.sent_final_accepted_count, 0) AS sent_final_accepted_count,
    COALESCE(ss.sent_final_pending_count, 0) AS sent_final_pending_count,
    COALESCE(ss.sent_final_rejected_count, 0) AS sent_final_rejected_count,
    COALESCE(ss.sent_repeated_request_count, 0) AS sent_repeated_request_count,
    COALESCE(ss.sent_request_first_24h_count, 0) AS sent_request_first_24h_count,
    COALESCE(ss.sent_request_first_72h_count, 0) AS sent_request_first_72h_count,
    COALESCE(ss.sent_request_first_7d_count, 0) AS sent_request_first_7d_count,
    ss.first_sent_request_at_raw,
    ss.last_sent_request_at_raw,
    COALESCE(rs.received_request_count, 0) AS received_request_count,
    COALESCE(rs.received_unique_partner_count, 0) AS received_unique_partner_count,
    COALESCE(rs.received_final_accepted_count, 0) AS received_final_accepted_count,
    COALESCE(rs.received_final_pending_count, 0) AS received_final_pending_count,
    COALESCE(rs.received_final_rejected_count, 0) AS received_final_rejected_count,
    COALESCE(rs.received_request_first_24h_count, 0) AS received_request_first_24h_count,
    COALESCE(rs.received_request_first_72h_count, 0) AS received_request_first_72h_count,
    COALESCE(rs.received_request_first_7d_count, 0) AS received_request_first_7d_count,
    rs.first_received_request_at_raw,
    rs.last_received_request_at_raw,
    COALESCE(ss.sent_request_count,0)+COALESCE(rs.received_request_count,0)
        AS total_request_endpoint_activity_count,
    (COALESCE(ss.sent_request_count,0)+COALESCE(rs.received_request_count,0)>0)
        AS has_any_friend_request_activity_flag,
    CAST(
        CASE WHEN COALESCE(ss.sent_request_count,0)=0 THEN NULL
             ELSE CAST(COALESCE(ss.sent_final_accepted_count,0) AS DECIMAL(20,6))
                  /NULLIF(CAST(ss.sent_request_count AS DECIMAL(20,6)),0)
        END AS DECIMAL(18,10)
    ) AS sent_eventual_acceptance_rate_including_pending,
    CAST(
        CASE WHEN COALESCE(ss.sent_final_accepted_count,0)
                     +COALESCE(ss.sent_final_rejected_count,0)=0 THEN NULL
             ELSE CAST(COALESCE(ss.sent_final_accepted_count,0) AS DECIMAL(20,6))
                  /NULLIF(
                      CAST(
                          COALESCE(ss.sent_final_accepted_count,0)
                          +COALESCE(ss.sent_final_rejected_count,0)
                          AS DECIMAL(20,6)
                      ),
                      0
                  )
        END AS DECIMAL(18,10)
    ) AS sent_eventual_decision_acceptance_rate,
    CAST(
        CASE WHEN COALESCE(rs.received_request_count,0)=0 THEN NULL
             ELSE CAST(COALESCE(rs.received_final_accepted_count,0) AS DECIMAL(20,6))
                  /NULLIF(CAST(rs.received_request_count AS DECIMAL(20,6)),0)
        END AS DECIMAL(18,10)
    ) AS received_eventual_acceptance_rate_including_pending
FROM votes_mart.mart_user_acquisition_profile_v2 AS p
LEFT JOIN sent_summary AS ss ON ss.user_id=p.user_id
LEFT JOIN received_summary AS rs ON rs.user_id=p.user_id;

ALTER TABLE votes_mart.mart_user_viral_profile_v2
    ADD PRIMARY KEY (user_id),
    ADD INDEX idx_user_viral_school_signup (current_school_id, signup_at),
    ADD INDEX idx_user_viral_request_flag (has_any_friend_request_activity_flag),
    ADD INDEX idx_user_viral_sent (sent_request_count),
    ADD INDEX idx_user_viral_received (received_request_count);


/* ============================================================================
   3. 학교 × 날짜 일별 패널 — 학교 ID와 수치만 저장

   학교 주소·유형 같은 문자열은 dim_school_current_v2에 1회만 저장한다.
   현재 roster 기준 학교를 요청 발생 시점의 학교라고 해석하면 안 된다.
============================================================================ */

DROP TABLE IF EXISTS votes_mart.mart_school_viral_daily_v2;
SET SESSION cte_max_recursion_depth = 5000;

CREATE TABLE votes_mart.mart_school_viral_daily_v2 AS
WITH RECURSIVE
date_bounds AS (
    SELECT MIN(min_date) AS min_date, MAX(max_date) AS max_date
    FROM (
        SELECT MIN(DATE(signup_at)) min_date, MAX(DATE(signup_at)) max_date
        FROM votes_mart.mart_user_acquisition_profile_v2
        UNION ALL
        SELECT MIN(request_created_date_raw), MAX(request_created_date_raw)
        FROM votes_mart.mart_friend_request_event_v2
        UNION ALL
        SELECT MIN(request_updated_date_raw), MAX(request_updated_date_raw)
        FROM votes_mart.mart_friend_request_event_v2
    ) AS ranges
),
calendar AS (
    SELECT min_date AS activity_date
    FROM date_bounds
    WHERE min_date IS NOT NULL
    UNION ALL
    SELECT DATE_ADD(c.activity_date, INTERVAL 1 DAY)
    FROM calendar AS c
    CROSS JOIN date_bounds AS b
    WHERE c.activity_date < b.max_date
),
school_context AS (
    SELECT
        current_school_id AS school_id,
        COUNT(*) AS current_roster_account_count,
        SUM(is_staff=0 AND is_superuser=0) AS current_roster_nonstaff_account_count,
        MIN(signup_at) AS current_roster_first_signup_at,
        MIN(current_roster_40th_signup_at) AS current_roster_40th_signup_at,
        MAX(signup_at) AS current_roster_last_signup_at
    FROM votes_mart.mart_user_acquisition_profile_v2
    WHERE current_school_id IS NOT NULL
    GROUP BY current_school_id
),
signup_daily AS (
    SELECT
        current_school_id AS school_id,
        DATE(signup_at) AS activity_date,
        COUNT(*) AS new_account_count,
        SUM(is_staff=0 AND is_superuser=0) AS new_nonstaff_account_count,
        SUM(is_staff=1 OR is_superuser=1) AS new_staff_or_superuser_account_count
    FROM votes_mart.mart_user_acquisition_profile_v2
    WHERE current_school_id IS NOT NULL AND signup_at IS NOT NULL
    GROUP BY current_school_id, DATE(signup_at)
),
request_sent_daily AS (
    SELECT
        sender.current_school_id AS school_id,
        f.request_created_date_raw AS activity_date,
        COUNT(*) AS sent_request_created_count,
        COUNT(DISTINCT f.send_user_id) AS sent_request_user_count,
        SUM(f.final_status_code='A') AS sent_created_final_accepted_count,
        SUM(f.final_status_code='P') AS sent_created_final_pending_count,
        SUM(f.final_status_code='R') AS sent_created_final_rejected_count,
        SUM(receiver.current_school_id=sender.current_school_id) AS sent_within_school_count_current,
        SUM(receiver.current_school_id<>sender.current_school_id) AS sent_cross_school_count_current,
        SUM(receiver.current_school_id IS NULL) AS sent_unknown_school_relation_count
    FROM votes_mart.mart_friend_request_event_v2 AS f
    JOIN votes_mart.mart_user_acquisition_profile_v2 AS sender
      ON sender.user_id=f.send_user_id
    LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS receiver
      ON receiver.user_id=f.receive_user_id
    WHERE sender.current_school_id IS NOT NULL
      AND f.request_created_date_raw IS NOT NULL
    GROUP BY sender.current_school_id, f.request_created_date_raw
),
request_received_daily AS (
    SELECT
        receiver.current_school_id AS school_id,
        f.request_created_date_raw AS activity_date,
        COUNT(*) AS received_request_created_count,
        COUNT(DISTINCT f.receive_user_id) AS received_request_user_count,
        SUM(f.final_status_code='A') AS received_created_final_accepted_count,
        SUM(f.final_status_code='P') AS received_created_final_pending_count,
        SUM(f.final_status_code='R') AS received_created_final_rejected_count,
        SUM(sender.current_school_id=receiver.current_school_id) AS received_within_school_count_current,
        SUM(sender.current_school_id<>receiver.current_school_id) AS received_cross_school_count_current,
        SUM(sender.current_school_id IS NULL) AS received_unknown_school_relation_count
    FROM votes_mart.mart_friend_request_event_v2 AS f
    JOIN votes_mart.mart_user_acquisition_profile_v2 AS receiver
      ON receiver.user_id=f.receive_user_id
    LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS sender
      ON sender.user_id=f.send_user_id
    WHERE receiver.current_school_id IS NOT NULL
      AND f.request_created_date_raw IS NOT NULL
    GROUP BY receiver.current_school_id, f.request_created_date_raw
),
request_update_sent_daily AS (
    SELECT
        sender.current_school_id AS school_id,
        f.request_updated_date_raw AS activity_date,
        COUNT(*) AS sent_request_last_updated_count,
        SUM(f.final_status_code='A') AS sent_final_accepted_last_updated_count,
        SUM(f.final_status_code='P') AS sent_final_pending_last_updated_count,
        SUM(f.final_status_code='R') AS sent_final_rejected_last_updated_count
    FROM votes_mart.mart_friend_request_event_v2 AS f
    JOIN votes_mart.mart_user_acquisition_profile_v2 AS sender
      ON sender.user_id=f.send_user_id
    WHERE sender.current_school_id IS NOT NULL
      AND f.request_updated_date_raw IS NOT NULL
    GROUP BY sender.current_school_id, f.request_updated_date_raw
),
request_update_received_daily AS (
    SELECT
        receiver.current_school_id AS school_id,
        f.request_updated_date_raw AS activity_date,
        COUNT(*) AS received_request_last_updated_count,
        SUM(f.final_status_code='A') AS received_final_accepted_last_updated_count,
        SUM(f.final_status_code='P') AS received_final_pending_last_updated_count,
        SUM(f.final_status_code='R') AS received_final_rejected_last_updated_count
    FROM votes_mart.mart_friend_request_event_v2 AS f
    JOIN votes_mart.mart_user_acquisition_profile_v2 AS receiver
      ON receiver.user_id=f.receive_user_id
    WHERE receiver.current_school_id IS NOT NULL
      AND f.request_updated_date_raw IS NOT NULL
    GROUP BY receiver.current_school_id, f.request_updated_date_raw
),
daily_base AS (
    SELECT
        s.school_id,
        c.activity_date,
        COALESCE(sc.current_roster_account_count,0) AS current_roster_account_count,
        COALESCE(sc.current_roster_nonstaff_account_count,0) AS current_roster_nonstaff_account_count,
        sc.current_roster_first_signup_at,
        sc.current_roster_40th_signup_at,
        sc.current_roster_last_signup_at,
        (sc.current_roster_40th_signup_at IS NOT NULL) AS current_roster_reached_40_flag,
        COALESCE(sd.new_account_count,0) AS new_account_count_current_roster,
        COALESCE(sd.new_nonstaff_account_count,0) AS new_nonstaff_account_count_current_roster,
        COALESCE(sd.new_staff_or_superuser_account_count,0) AS new_staff_or_superuser_account_count_current_roster,
        COALESCE(rs.sent_request_created_count,0) AS sent_request_created_count,
        COALESCE(rs.sent_request_user_count,0) AS sent_request_user_count,
        COALESCE(rs.sent_created_final_accepted_count,0) AS sent_created_final_accepted_count,
        COALESCE(rs.sent_created_final_pending_count,0) AS sent_created_final_pending_count,
        COALESCE(rs.sent_created_final_rejected_count,0) AS sent_created_final_rejected_count,
        COALESCE(rs.sent_within_school_count_current,0) AS sent_within_school_count_current,
        COALESCE(rs.sent_cross_school_count_current,0) AS sent_cross_school_count_current,
        COALESCE(rs.sent_unknown_school_relation_count,0) AS sent_unknown_school_relation_count,
        COALESCE(rr.received_request_created_count,0) AS received_request_created_count,
        COALESCE(rr.received_request_user_count,0) AS received_request_user_count,
        COALESCE(rr.received_created_final_accepted_count,0) AS received_created_final_accepted_count,
        COALESCE(rr.received_created_final_pending_count,0) AS received_created_final_pending_count,
        COALESCE(rr.received_created_final_rejected_count,0) AS received_created_final_rejected_count,
        COALESCE(rr.received_within_school_count_current,0) AS received_within_school_count_current,
        COALESCE(rr.received_cross_school_count_current,0) AS received_cross_school_count_current,
        COALESCE(rr.received_unknown_school_relation_count,0) AS received_unknown_school_relation_count,
        COALESCE(us.sent_request_last_updated_count,0) AS sent_request_last_updated_count,
        COALESCE(us.sent_final_accepted_last_updated_count,0) AS sent_final_accepted_last_updated_count,
        COALESCE(us.sent_final_pending_last_updated_count,0) AS sent_final_pending_last_updated_count,
        COALESCE(us.sent_final_rejected_last_updated_count,0) AS sent_final_rejected_last_updated_count,
        COALESCE(ur.received_request_last_updated_count,0) AS received_request_last_updated_count,
        COALESCE(ur.received_final_accepted_last_updated_count,0) AS received_final_accepted_last_updated_count,
        COALESCE(ur.received_final_pending_last_updated_count,0) AS received_final_pending_last_updated_count,
        COALESCE(ur.received_final_rejected_last_updated_count,0) AS received_final_rejected_last_updated_count
    FROM votes_mart.dim_school_current_v2 AS s
    CROSS JOIN calendar AS c
    LEFT JOIN school_context AS sc ON sc.school_id=s.school_id
    LEFT JOIN signup_daily AS sd
      ON sd.school_id=s.school_id AND sd.activity_date=c.activity_date
    LEFT JOIN request_sent_daily AS rs
      ON rs.school_id=s.school_id AND rs.activity_date=c.activity_date
    LEFT JOIN request_received_daily AS rr
      ON rr.school_id=s.school_id AND rr.activity_date=c.activity_date
    LEFT JOIN request_update_sent_daily AS us
      ON us.school_id=s.school_id AND us.activity_date=c.activity_date
    LEFT JOIN request_update_received_daily AS ur
      ON ur.school_id=s.school_id AND ur.activity_date=c.activity_date
)
SELECT
    db.*,
    SUM(db.new_account_count_current_roster) OVER (
        PARTITION BY db.school_id ORDER BY db.activity_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_account_count_current_roster,
    SUM(db.new_nonstaff_account_count_current_roster) OVER (
        PARTITION BY db.school_id ORDER BY db.activity_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_nonstaff_account_count_current_roster,
    SUM(db.sent_request_created_count) OVER (
        PARTITION BY db.school_id ORDER BY db.activity_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_sent_request_created_count,
    SUM(db.received_request_created_count) OVER (
        PARTITION BY db.school_id ORDER BY db.activity_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_received_request_created_count,
    CAST(
        CASE WHEN db.sent_request_created_count=0 THEN NULL
             ELSE CAST(db.sent_created_final_accepted_count AS DECIMAL(20,6))
                  /NULLIF(CAST(db.sent_request_created_count AS DECIMAL(20,6)),0)
        END AS DECIMAL(18,10)
    ) AS sent_eventual_acceptance_rate_including_pending,
    CAST(
        CASE WHEN db.sent_created_final_accepted_count+db.sent_created_final_rejected_count=0
             THEN NULL
             ELSE CAST(db.sent_created_final_accepted_count AS DECIMAL(20,6))
                  /NULLIF(
                      CAST(
                          db.sent_created_final_accepted_count+db.sent_created_final_rejected_count
                          AS DECIMAL(20,6)
                      ),
                      0
                  )
        END AS DECIMAL(18,10)
    ) AS sent_eventual_decision_acceptance_rate,
    CAST(
        CASE WHEN db.received_request_created_count=0 THEN NULL
             ELSE CAST(db.received_created_final_accepted_count AS DECIMAL(20,6))
                  /NULLIF(CAST(db.received_request_created_count AS DECIMAL(20,6)),0)
        END AS DECIMAL(18,10)
    ) AS received_eventual_acceptance_rate_including_pending,
    DATEDIFF(db.activity_date, DATE(db.current_roster_first_signup_at))
        AS days_from_first_signup_current_roster,
    DATEDIFF(db.activity_date, DATE(db.current_roster_40th_signup_at))
        AS days_from_40th_signup_current_roster,
    (db.current_roster_40th_signup_at IS NOT NULL
     AND db.activity_date<DATE(db.current_roster_40th_signup_at))
        AS before_observed_40th_signup_flag,
    (db.current_roster_40th_signup_at IS NOT NULL
     AND db.activity_date>=DATE(db.current_roster_40th_signup_at))
        AS on_or_after_observed_40th_signup_flag
FROM daily_base AS db;

ALTER TABLE votes_mart.mart_school_viral_daily_v2
    ADD PRIMARY KEY (school_id, activity_date),
    ADD INDEX idx_school_daily_date (activity_date),
    ADD INDEX idx_school_daily_40day (days_from_40th_signup_current_roster),
    ADD INDEX idx_school_daily_signup (new_account_count_current_roster),
    ADD INDEX idx_school_daily_request
        (sent_request_created_count, received_request_created_count);


/* ============================================================================
   4. QA
============================================================================ */

SELECT
    (SELECT COUNT(*) FROM final.accounts_friendrequest) AS source_request_rows,
    (SELECT COUNT(*) FROM votes_mart.mart_friend_request_event_v2) AS mart_request_rows,
    (SELECT COUNT(DISTINCT request_id)
       FROM votes_mart.mart_friend_request_event_v2) AS mart_distinct_request_ids,
    (SELECT COUNT(*)-COUNT(DISTINCT request_id)
       FROM votes_mart.mart_friend_request_event_v2) AS mart_duplicate_request_rows,
    (SELECT COUNT(*) FROM final.accounts_user) AS source_user_rows,
    (SELECT COUNT(*) FROM votes_mart.mart_user_viral_profile_v2) AS profile_user_rows,
    (SELECT COUNT(*)-COUNT(DISTINCT school_id,activity_date)
       FROM votes_mart.mart_school_viral_daily_v2) AS school_day_duplicate_rows;

SELECT
    final_status_code,
    COUNT(*) AS request_rows,
    COUNT(DISTINCT send_user_id) AS sender_count,
    COUNT(DISTINCT receive_user_id) AS receiver_count
FROM votes_mart.mart_friend_request_event_v2
GROUP BY final_status_code
ORDER BY final_status_code;
