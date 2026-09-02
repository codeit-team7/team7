/* ============================================================================
   SNS FINAL MART: EXTRACT + QA RUNBOOK
   MySQL 8.0 / source schema: final / mart schema: votes_mart
   Prepared: 2026-09-02

   Purpose
   - GitHub에서 최종 마트의 사용·추출·QA 기준을 한 파일로 관리한다.
   - 기본 실행 구간은 SELECT만 포함하며 원천/마트 데이터를 변경하지 않는다.
   - 현재 로컬 서버는 secure_file_priv=NULL이므로 CSV는 인증된 mysql
     클라이언트에서 스트리밍 추출한다.

   Final marts
   1) mart_funnel_session          grain: session_id x funnel_type
   2) mart_question_exposure       grain: question_set_id x question_position
   3) mart_user_network_snapshot   grain: user_id

   Important
   - CSV의 물리적 정렬 순서는 보장하지 않는다.
   - 연결요소/PageRank는 전체 그래프 연산이므로 이 SQL 마트 범위가 아니다.
   - QA_EXPECTED 주석은 2026-09-02 로컬 스냅샷에서 확인한 기준값이다.
============================================================================ */

SET NAMES utf8mb4;
SET SESSION group_concat_max_len = 1024 * 1024;

/* ============================================================================
   0. INVENTORY / SCHEMA SNAPSHOT
============================================================================ */

SELECT
    table_name,
    table_rows AS optimizer_estimated_rows,
    create_time,
    update_time
FROM information_schema.tables
WHERE table_schema = 'votes_mart'
  AND table_name IN (
      'mart_funnel_session',
      'mart_question_exposure',
      'mart_user_network_snapshot'
  )
ORDER BY table_name;

SELECT
    table_name,
    ordinal_position,
    column_name,
    column_type,
    is_nullable,
    column_key,
    extra
FROM information_schema.columns
WHERE table_schema = 'votes_mart'
  AND table_name IN (
      'mart_funnel_session',
      'mart_question_exposure',
      'mart_user_network_snapshot'
  )
ORDER BY table_name, ordinal_position;

/* ============================================================================
   1. ANALYSIS EXTRACTION QUERIES
   필요 시 WHERE 절을 추가한다. 대용량 결과는 mysql 화면에 직접 출력하지 말고
   01_export_marts_server.sql로 내보낸다.
============================================================================ */

-- 1-A. Session funnel mart
SELECT *
FROM votes_mart.mart_funnel_session;

-- 1-B. Question exposure mart
SELECT *
FROM votes_mart.mart_question_exposure;

-- 1-C. User network snapshot mart
SELECT *
FROM votes_mart.mart_user_network_snapshot;

/* ============================================================================
   2. FINAL GRAIN / KEY QA
   QA_EXPECTED:
   - funnel   215,148 rows / 215,148 unique grains / duplicate 0
   - question 1,583,840 rows / 1,583,840 unique grains / duplicate 0
   - network  677,085 rows / 677,085 unique grains / duplicate 0
============================================================================ */

SELECT
    'mart_funnel_session' AS mart_name,
    'session_id x funnel_type' AS grain,
    COUNT(*) AS row_count,
    COUNT(DISTINCT session_id, funnel_type) AS unique_grain_count,
    COUNT(*) - COUNT(DISTINCT session_id, funnel_type) AS duplicate_grain_count,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT session_id, funnel_type) THEN 'PASS'
        ELSE 'FAIL'
    END AS qa_status
FROM votes_mart.mart_funnel_session

UNION ALL

SELECT
    'mart_question_exposure',
    'question_set_id x question_position',
    COUNT(*),
    COUNT(DISTINCT question_set_id, question_position),
    COUNT(*) - COUNT(DISTINCT question_set_id, question_position),
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT question_set_id, question_position) THEN 'PASS'
        ELSE 'FAIL'
    END
FROM votes_mart.mart_question_exposure

UNION ALL

SELECT
    'mart_user_network_snapshot',
    'user_id',
    COUNT(*),
    COUNT(DISTINCT user_id),
    COUNT(*) - COUNT(DISTINCT user_id),
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT user_id) THEN 'PASS'
        ELSE 'FAIL'
    END
FROM votes_mart.mart_user_network_snapshot;

/* ============================================================================
   3. FUNNEL MART QA

   Event mapping
   - SIGNUP:        view_signup            -> complete_signup
   - QUESTION:      click_question_start   -> complete_question
   - PING_REACTION: click_question_open    -> click_question_share
   - PURCHASE:      view_shop              -> complete_purchase

   ordered_conversion_flag를 대표 전환 지표로 사용한다. 동일 세션 안에서 완료 이벤트가
   진입 이벤트보다 먼저 기록된 경우 단순 converted_flag는 1이어도 ordered는 0일 수 있다.
============================================================================ */

-- Included source event volume and session coverage.
-- QA_EXPECTED included events: 1,783,964
SELECT
    event_key,
    COUNT(*) AS event_count,
    COUNT(DISTINCT session_id) AS session_count,
    MIN(event_datetime) AS first_event_at,
    MAX(event_datetime) AS last_event_at
FROM final.hackle_events
WHERE event_key IN (
    'view_signup',
    'complete_signup',
    'click_question_start',
    'skip_question',
    'complete_question',
    'click_question_open',
    'click_question_share',
    'view_shop',
    'click_purchase',
    'complete_purchase'
)
GROUP BY event_key
ORDER BY event_key;

-- Session identity bridge QA.
-- QA_EXPECTED total sessions 253,616 / numeric 145,808 / ambiguous 87,793 /
--             no_user 11,171 / anonymous_id 8,844
SELECT
    identity_status,
    COUNT(*) AS session_count
FROM votes_mart.int_hackle_session_user
GROUP BY identity_status
ORDER BY session_count DESC;

-- Final funnel distribution.
-- QA_EXPECTED ordered conversion rates:
-- PING_REACTION 7.72%, PURCHASE 9.37%, QUESTION 65.89%, SIGNUP 12.78%
SELECT
    funnel_type,
    COUNT(*) AS session_count,
    SUM(started_flag) AS started_session_count,
    SUM(converted_flag) AS converted_session_count,
    SUM(ordered_conversion_flag) AS ordered_converted_session_count,
    ROUND(
        100.0 * SUM(ordered_conversion_flag) / NULLIF(SUM(started_flag), 0),
        2
    ) AS ordered_conversion_rate_pct
FROM votes_mart.mart_funnel_session
GROUP BY funnel_type
ORDER BY funnel_type;

-- Timing sanity checks. Negative times must not be counted as ordered conversions.
SELECT
    COUNT(*) AS invalid_ordered_conversion_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS qa_status
FROM votes_mart.mart_funnel_session
WHERE ordered_conversion_flag = 1
  AND first_conversion_at < first_entry_at;

/* ============================================================================
   4. QUESTION EXPOSURE MART QA

   JSON array position is 0-based in JSON_TABLE and stored as 1-based
   question_position in the mart. Historical question-piece IDs that are absent
   from the current polls_questionpiece table are retained with piece_exists_flag=0.
============================================================================ */

-- Question-set status distribution.
-- QA_EXPECTED: F 153,411 / O 4,407 / C 566
SELECT
    status,
    COUNT(*) AS question_set_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS share_pct
FROM final.polls_questionset
GROUP BY status
ORDER BY status;

-- JSON shape check. QA_EXPECTED 158,384 sets and 1,583,840 elements (10 each).
SELECT
    COUNT(*) AS question_set_count,
    SUM(JSON_LENGTH(question_piece_id_list)) AS total_question_position_count,
    MIN(JSON_LENGTH(question_piece_id_list)) AS min_position_count,
    MAX(JSON_LENGTH(question_piece_id_list)) AS max_position_count,
    SUM(JSON_VALID(question_piece_id_list) = 0) AS invalid_json_count
FROM final.polls_questionset;

-- Current-source missing piece coverage.
-- QA_EXPECTED missing_piece_count 318,364 (20.10% of all positions).
SELECT
    COUNT(*) AS total_position_count,
    SUM(piece_exists_flag = 1) AS existing_piece_count,
    SUM(piece_exists_flag = 0) AS missing_piece_count,
    ROUND(100.0 * SUM(piece_exists_flag = 0) / COUNT(*), 2) AS missing_piece_rate_pct
FROM votes_mart.mart_question_exposure;

-- Position denominator must use existing pieces when calculating current content metrics.
SELECT
    question_position,
    COUNT(*) AS position_row_count,
    SUM(piece_exists_flag = 1) AS existing_piece_denominator,
    SUM(piece_exists_flag = 0) AS historical_missing_piece_count
FROM votes_mart.mart_question_exposure
GROUP BY question_position
ORDER BY question_position;

-- Hackle actual skip log coverage.
-- QA_EXPECTED 454,981 skip events / 449,484 with question_id /
--             3,897 distinct questions / 39,423 sessions
SELECT
    COUNT(*) AS skip_event_count,
    SUM(question_id IS NOT NULL) AS skip_event_with_question_id_count,
    COUNT(DISTINCT question_id) AS skipped_question_count,
    COUNT(DISTINCT session_id) AS skip_session_count
FROM final.hackle_events
WHERE event_key = 'skip_question';

/* ============================================================================
   5. USER NETWORK MART QA

   Definitions
   - raw listing: accounts_user.friend_id_list의 JSON 원소
   - valid directed edge: source와 target이 현재 accounts_user에 존재하고 self-edge가 아님
   - undirected relationship: LEAST/GREATEST(user_id)로 정규화한 한 쌍
   - mutual: 양쪽 사용자 모두 상대를 현재 친구 목록에 기록
   - one-way: 한쪽만 기록
   - orphan: target 사용자가 현재 accounts_user에 없음
============================================================================ */

-- Source user / JSON health.
-- QA_EXPECTED users 677,085 / raw elements 36,107,386 / invalid JSON 0 /
--             null list 0 / empty list 0
SELECT
    COUNT(*) AS user_count,
    SUM(friend_id_list IS NULL) AS null_friend_list_count,
    SUM(JSON_VALID(friend_id_list) = 0) AS invalid_json_count,
    SUM(JSON_LENGTH(friend_id_list) = 0) AS empty_friend_list_count,
    SUM(JSON_LENGTH(friend_id_list)) AS raw_friend_list_element_count
FROM final.accounts_user;

-- Directed-edge health.
-- QA_EXPECTED unique listed directed 36,107,018 / valid directed 36,098,507 /
--             duplicate refs 368 / orphan 8,511 /
--             orphan target IDs 2,839 / affected users 7,655
SELECT
    (SELECT COUNT(*)
       FROM votes_mart.int_current_friend_edges_directed) AS unique_listed_directed_edge_count,
    (SELECT COUNT(*)
       FROM votes_mart.int_current_friend_edges_directed)
    -
    (SELECT COUNT(*)
       FROM votes_mart.int_current_friend_edges_orphan) AS valid_directed_edge_count,
    (36107386 - (SELECT COUNT(*)
                   FROM votes_mart.int_current_friend_edges_directed)) AS raw_minus_unique_count,
    (SELECT COUNT(*)
       FROM votes_mart.int_current_friend_edges_orphan) AS orphan_edge_count,
    (SELECT COUNT(DISTINCT missing_target_user_id)
       FROM votes_mart.int_current_friend_edges_orphan) AS missing_target_user_count,
    (SELECT COUNT(DISTINCT source_user_id)
       FROM votes_mart.int_current_friend_edges_orphan) AS affected_source_user_count;

-- Self-edge check. QA_EXPECTED 0.
SELECT
    COUNT(*) AS self_edge_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS qa_status
FROM votes_mart.int_current_friend_edges_directed
WHERE source_user_id = target_user_id;

-- Undirected relationship / reciprocity.
-- QA_EXPECTED relationships 18,136,949 / mutual 17,961,558 /
--             one-way 175,391 / reconstructed directed 36,098,507 /
--             mutual rate 99.03%
SELECT
    COUNT(*) AS undirected_relationship_count,
    SUM(is_mutual = 1) AS mutual_relationship_count,
    SUM(is_mutual = 0) AS one_way_relationship_count,
    SUM(direction_count) AS reconstructed_directed_edge_count,
    ROUND(100.0 * SUM(is_mutual = 1) / COUNT(*), 2) AS mutual_relationship_rate_pct
FROM votes_mart.int_current_friend_edges_undirected;

-- Friend-request status totals.
-- QA_EXPECTED total 17,147,175 / A 12,878,407 / P 3,938,608 / R 330,160
SELECT
    status,
    COUNT(*) AS request_record_count
FROM final.accounts_friendrequest
GROUP BY status
ORDER BY status;

-- Accepted request pair de-duplication.
-- QA_EXPECTED accepted records 12,878,407 / unique pairs 12,875,445 /
--             duplicate records 2,962
SELECT
    COUNT(*) AS accepted_request_record_count,
    COUNT(DISTINCT LEAST(send_user_id, receive_user_id),
                   GREATEST(send_user_id, receive_user_id)) AS unique_accepted_pair_count,
    COUNT(*) - COUNT(DISTINCT LEAST(send_user_id, receive_user_id),
                              GREATEST(send_user_id, receive_user_id))
        AS duplicate_accepted_request_count
FROM final.accounts_friendrequest
WHERE status = 'A'
  AND send_user_id <> receive_user_id;

-- Final per-user reconciliation. QA_EXPECTED all error counts = 0.
SELECT
    COUNT(*) AS user_count,
    COUNT(DISTINCT user_id) AS unique_user_count,
    SUM(
        current_friend_count_snapshot <>
        valid_outbound_listing_count
        + orphan_friend_reference_count
        + duplicate_friend_reference_count
    ) AS friend_count_reconciliation_error_users,
    SUM(
        network_degree_count <>
        mutual_friend_count + one_way_friend_count
    ) AS degree_reconciliation_error_users,
    SUM(
        accepted_request_record_count <>
        accepted_request_partner_count + repeated_accepted_request_count
    ) AS accepted_request_reconciliation_error_users
FROM votes_mart.mart_user_network_snapshot;

-- Final network headline metrics.
SELECT
    COUNT(*) AS user_count,
    SUM(current_friend_count_snapshot) AS raw_friend_list_element_count,
    SUM(valid_outbound_listing_count) AS valid_outbound_listing_count,
    SUM(valid_inbound_listing_count) AS valid_inbound_listing_count,
    SUM(network_degree_count) AS network_degree_endpoint_count,
    SUM(mutual_friend_count) AS mutual_friend_endpoint_count,
    SUM(one_way_friend_count) AS one_way_friend_endpoint_count,
    SUM(orphan_friend_reference_count) AS orphan_reference_count,
    SUM(duplicate_friend_reference_count) AS duplicate_reference_count,
    SUM(is_isolated_valid_network = 1) AS valid_network_isolated_user_count,
    SUM(is_inbound_only_user = 1) AS inbound_only_user_count,
    SUM(total_request_activity_count) AS request_activity_endpoint_count,
    SUM(accepted_request_partner_count) AS accepted_partner_endpoint_count
FROM votes_mart.mart_user_network_snapshot;

/* ============================================================================
   6. ALL-MART RELEASE GATE
   A release is PASS only when every final grain is unique and the three network
   reconciliation counts are all zero.
============================================================================ */

WITH grain_qa AS (
    SELECT
        COUNT(*) - COUNT(DISTINCT session_id, funnel_type) AS funnel_dup,
        (SELECT COUNT(*) - COUNT(DISTINCT question_set_id, question_position)
           FROM votes_mart.mart_question_exposure) AS question_dup,
        (SELECT COUNT(*) - COUNT(DISTINCT user_id)
           FROM votes_mart.mart_user_network_snapshot) AS network_dup
    FROM votes_mart.mart_funnel_session
),
network_qa AS (
    SELECT
        SUM(
            current_friend_count_snapshot <>
            valid_outbound_listing_count
            + orphan_friend_reference_count
            + duplicate_friend_reference_count
        ) AS friend_count_error,
        SUM(
            network_degree_count <>
            mutual_friend_count + one_way_friend_count
        ) AS degree_error,
        SUM(
            accepted_request_record_count <>
            accepted_request_partner_count + repeated_accepted_request_count
        ) AS accepted_request_error
    FROM votes_mart.mart_user_network_snapshot
)
SELECT
    grain_qa.*,
    network_qa.*,
    CASE
        WHEN funnel_dup = 0
         AND question_dup = 0
         AND network_dup = 0
         AND friend_count_error = 0
         AND degree_error = 0
         AND accepted_request_error = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS release_gate_status
FROM grain_qa
CROSS JOIN network_qa;

/* ============================================================================
   7. CSV EXPORT

   The current local server has secure_file_priv=NULL, so SELECT ... INTO OUTFILE
   is intentionally not used. The delivery process runs SELECT JSON_ARRAY(...)
   through an authenticated mysql client, streams rows to UTF-8 CSV, prepends
   ordered headers from information_schema.columns, and validates row/column
   counts plus final grain uniqueness.
============================================================================ */
