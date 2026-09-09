/* ============================================================================
   03. 질문 후보·선택·관계 v2 — compact star schema / 디스크 안전형

   왜 분리하는가
   - polls_usercandidate 약 476.96만 행마다 질문 문구, 사용자 속성, 학교 속성,
     친구 JSON, 질문세트 목록을 반복 저장하지 않는다.
   - 가장 큰 fact에는 원초 후보 식별자와 키·시각·중복 진단만 저장한다.
   - 질문, 질문조각, 질문세트, 세트-조각 위치, UQR, 질문자 매핑,
     질문자-후보 관계는 각각 자기 grain으로 한 번만 저장한다.
   - 분석 편의 컬럼은 마지막 VIEW에서 조합한다. VIEW는 물리 복제 용량이 없다.

   생성 객체와 grain
   1) dim_question_v2
      - question_id 1행
   2) mart_question_piece_record_v2
      - question_piece_id 1행
   3) mart_question_set_record_v2
      - question_set_id 1행
   4) bridge_question_set_piece_v2
      - question_set_id × question_position 1행
   5) mart_vote_record_v2
      - accounts_userquestionrecord.id 1행
   6) bridge_question_piece_owner_v2
      - 현재 존재하는 question_piece_id 1행
   7) mart_question_candidate_exposure_v2
      - polls_usercandidate.id 1행; 원초 4,769,609행 전량 보존
   8) bridge_question_owner_candidate_relation_v2
      - 해석 가능한 owner_user_id × candidate_user_id 1행
   9) vw_question_candidate_analysis_v2
      - 후보 원행 1행을 유지하는 분석 편의 VIEW

   owner_resolution_code
   - 0: owner를 찾지 못함
   - 1: questionset owner와 UQR voter가 일치
   - 2: questionset owner만 존재
   - 3: questionset owner가 없어 UQR voter를 대체 사용
   - 4: questionset owner와 UQR voter가 충돌하여 owner 미확정
   - 5: 한 question_piece가 서로 다른 qset owner에 연결되어 owner 미확정

   관계 시점 주의
   - 친구·학교·학년·반은 기준시점이 알려지지 않은 현재 스냅샷이다.
   - friendrequest status는 최종 상태다. accepted 시점 이력이 아니므로
     updated_at은 accepted_effective_at_proxy로만 사용한다.
   - candidate_source_created_at 이전 proxy 플래그는 과거 친구 여부의 확정값이
     아니라, 그 시점 이전에 관련 요청/최종 accepted 레코드가 있었을 가능성을
     나타내는 대체지표다.
   - 질문세트 opening_time은 실제 화면 노출시각으로 확정되지 않았다.

   선행 실행
   - 01_user_acquisition_and_school_bridge_v2.sql
   - 02_friend_request_and_school_viral_v2.sql

   후속 호환
   - 06이 쓰는 mart_question_set_record_v2 / mart_vote_record_v2 컬럼 유지
   - 07이 쓰는 후보 fact의 id, piece, user, created_at,
     candidate_pair_occurrence_number 컬럼 유지

   변경 범위
   - 기존 7개 CSV 마트와 final 원천은 수정하지 않는다.
   - 아래 votes_mart의 _v2 객체만 재생성한다.
============================================================================ */

CREATE DATABASE IF NOT EXISTS votes_mart;


/* VIEW 의존성을 먼저 해제한다. */
DROP VIEW IF EXISTS votes_mart.vw_question_candidate_analysis_v2;


/* ============================================================================
   1. 질문 차원 — 긴 질문 문구는 question_id별 한 번만 저장
============================================================================ */

DROP TABLE IF EXISTS votes_mart.dim_question_v2;

CREATE TABLE votes_mart.dim_question_v2 AS
SELECT
    q.id AS question_id,
    q.question_text,
    q.created_at AS question_created_at
FROM final.polls_question AS q;

ALTER TABLE votes_mart.dim_question_v2
    ADD PRIMARY KEY (question_id),
    ADD INDEX idx_question_created (question_created_at);


/* ============================================================================
   2. 질문조각 원장 — question_text와 사용자 속성을 반복 저장하지 않음
============================================================================ */

DROP TABLE IF EXISTS votes_mart.mart_question_piece_record_v2;

CREATE TABLE votes_mart.mart_question_piece_record_v2 AS
SELECT
    qp.id AS question_piece_id,
    qp.question_id,
    qp.created_at AS question_piece_created_at,
    qp.is_voted AS raw_is_voted,
    qp.is_skipped AS raw_is_skipped,
    (q.question_id IS NULL) AS orphan_question_flag,
    (qp.is_voted=1 AND qp.is_skipped=1) AS voted_and_skipped_flag
FROM final.polls_questionpiece AS qp
LEFT JOIN votes_mart.dim_question_v2 AS q
  ON q.question_id = qp.question_id;

ALTER TABLE votes_mart.mart_question_piece_record_v2
    ADD PRIMARY KEY (question_piece_id),
    ADD INDEX idx_piece_question (question_id),
    ADD INDEX idx_piece_created (question_piece_created_at);


/* ============================================================================
   3. 질문세트 원장 — question_set_id 1행

   JSON 원문은 세트당 한 번만 저장한다. 사용자 profile은 붙이지 않는다.
============================================================================ */

DROP TABLE IF EXISTS votes_mart.mart_question_set_record_v2;

CREATE TABLE votes_mart.mart_question_set_record_v2 AS
SELECT
    qs.id AS question_set_id,
    qs.user_id AS question_set_owner_user_id,
    qs.status AS question_set_status,
    qs.created_at AS question_set_created_at,
    qs.opening_time AS question_set_opening_time,
    qs.question_piece_id_list AS question_piece_id_list_json,
    JSON_VALID(qs.question_piece_id_list) AS piece_list_json_valid_flag,
    CASE
        WHEN COALESCE(JSON_VALID(qs.question_piece_id_list),0)=0 THEN 0
        WHEN JSON_TYPE(qs.question_piece_id_list)='ARRAY' THEN 1
        ELSE 0
    END AS piece_list_array_valid_flag,
    CASE
        WHEN COALESCE(JSON_VALID(qs.question_piece_id_list),0)=0 THEN NULL
        WHEN JSON_TYPE(qs.question_piece_id_list)='ARRAY'
            THEN JSON_LENGTH(qs.question_piece_id_list)
        ELSE NULL
    END AS piece_list_length,
    TIMESTAMPDIFF(
        SECOND, qs.created_at, qs.opening_time
    ) AS opening_delay_seconds,
    (p.user_id IS NULL) AS orphan_owner_user_flag
FROM final.polls_questionset AS qs
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS p
  ON p.user_id = qs.user_id;

ALTER TABLE votes_mart.mart_question_set_record_v2
    ADD PRIMARY KEY (question_set_id),
    ADD INDEX idx_qset_owner_created (
        question_set_owner_user_id, question_set_created_at
    ),
    ADD INDEX idx_qset_status (question_set_status),
    ADD INDEX idx_qset_opening (question_set_opening_time);


/* ============================================================================
   4. 질문세트-질문조각 위치 bridge

   세트 JSON의 각 원소를 위치와 함께 보존한다. 현재 qpiece 원천에서 사라진
   참조도 LEFT JOIN flag로 남긴다. 한 qpiece가 여러 세트/위치에 나타나도
   삭제하거나 임의의 첫 행을 고르지 않는다.
============================================================================ */

DROP TABLE IF EXISTS votes_mart.bridge_question_set_piece_v2;

CREATE TABLE votes_mart.bridge_question_set_piece_v2 AS
SELECT
    qs.id AS question_set_id,
    jt.question_position,
    jt.question_piece_id,
    (qp.question_piece_id IS NOT NULL) AS question_piece_exists_flag
FROM final.polls_questionset AS qs
JOIN JSON_TABLE(
    CASE
        WHEN COALESCE(JSON_VALID(qs.question_piece_id_list),0)=0 THEN JSON_ARRAY()
        WHEN JSON_TYPE(qs.question_piece_id_list)<>'ARRAY' THEN JSON_ARRAY()
        ELSE qs.question_piece_id_list
    END,
    '$[*]' COLUMNS (
        question_position FOR ORDINALITY,
        question_piece_id BIGINT PATH '$' NULL ON EMPTY NULL ON ERROR
    )
) AS jt ON TRUE
LEFT JOIN votes_mart.mart_question_piece_record_v2 AS qp
  ON qp.question_piece_id = jt.question_piece_id;

ALTER TABLE votes_mart.bridge_question_set_piece_v2
    ADD PRIMARY KEY (question_set_id, question_position),
    ADD INDEX idx_qset_piece_piece (question_piece_id, question_set_id),
    ADD INDEX idx_qset_piece_missing (question_piece_exists_flag);


/* ============================================================================
   5. UQR / 투표·Ping 현재상태 원장

   긴 문구와 사용자 profile은 저장하지 않는다. has_read, answer_status,
   report_count 등은 해당 사건의 발생시각이 아니라 현재/최종 상태 필드다.
============================================================================ */

DROP TABLE IF EXISTS votes_mart.mart_vote_record_v2;

CREATE TABLE votes_mart.mart_vote_record_v2 AS
SELECT
    uqr.id AS user_question_record_id,
    uqr.user_id AS voter_user_id,
    uqr.chosen_user_id,
    uqr.question_id,
    uqr.question_piece_id,
    uqr.status AS vote_status_current,
    uqr.created_at AS vote_record_created_at,
    uqr.has_read AS ping_has_read_current,
    uqr.answer_status AS ping_answer_status_current,
    uqr.answer_updated_at AS ping_answer_status_updated_at,
    uqr.report_count AS ping_report_count_current,
    uqr.opened_times AS ping_opened_times_current,
    (voter.user_id IS NULL) AS orphan_voter_flag,
    (chosen.user_id IS NULL) AS orphan_chosen_user_flag,
    (qp.question_piece_id IS NULL) AS orphan_question_piece_flag,
    (q.question_id IS NULL) AS orphan_question_flag,
    (
        qp.question_id IS NOT NULL
        AND uqr.question_id IS NOT NULL
        AND qp.question_id<>uqr.question_id
    ) AS piece_question_mismatch_flag
FROM final.accounts_userquestionrecord AS uqr
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS voter
  ON voter.user_id = uqr.user_id
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS chosen
  ON chosen.user_id = uqr.chosen_user_id
LEFT JOIN votes_mart.mart_question_piece_record_v2 AS qp
  ON qp.question_piece_id = uqr.question_piece_id
LEFT JOIN votes_mart.dim_question_v2 AS q
  ON q.question_id = uqr.question_id;

ALTER TABLE votes_mart.mart_vote_record_v2
    ADD PRIMARY KEY (user_question_record_id),
    ADD UNIQUE KEY uk_vote_piece (question_piece_id),
    ADD INDEX idx_vote_voter_time (voter_user_id, vote_record_created_at),
    ADD INDEX idx_vote_chosen_time (chosen_user_id, vote_record_created_at),
    ADD INDEX idx_vote_question (question_id);


/* ============================================================================
   6. 질문조각별 질문자(owner) 해석 bridge

   qset JSON과 UQR voter를 question_piece 1행으로 축약한다. 세트가 여러 개면
   정보 손실을 숨기지 않고 개수·최솟값·최댓값·ambiguity flag를 보존한다.
   GROUP_CONCAT으로 목록 문자열을 만들지 않는다.
============================================================================ */

DROP TABLE IF EXISTS votes_mart.bridge_question_piece_owner_v2;

CREATE TABLE votes_mart.bridge_question_piece_owner_v2 AS
WITH qset_rollup AS (
    SELECT
        sp.question_piece_id,
        COUNT(*) AS question_set_membership_row_count,
        COUNT(DISTINCT sp.question_set_id) AS question_set_id_count,
        COUNT(DISTINCT qs.question_set_owner_user_id)
            AS question_set_owner_user_count,
        CASE WHEN COUNT(*)=1
             THEN MAX(sp.question_set_id) END AS single_question_set_id,
        CASE WHEN COUNT(*)=1
             THEN MAX(sp.question_position) END AS single_question_position,
        CASE WHEN COUNT(DISTINCT qs.question_set_owner_user_id)=1
             THEN MAX(qs.question_set_owner_user_id) END
             AS unambiguous_qset_owner_user_id,
        MIN(sp.question_set_id) AS question_set_id_min,
        MAX(sp.question_set_id) AS question_set_id_max
    FROM votes_mart.bridge_question_set_piece_v2 AS sp
    JOIN votes_mart.mart_question_set_record_v2 AS qs
      ON qs.question_set_id = sp.question_set_id
    WHERE sp.question_piece_id IS NOT NULL
    GROUP BY sp.question_piece_id
), owner_base AS (
    SELECT
        qp.question_piece_id,
        COALESCE(qs.question_set_membership_row_count,0)
            AS question_set_membership_row_count,
        COALESCE(qs.question_set_id_count,0) AS question_set_id_count,
        COALESCE(qs.question_set_owner_user_count,0)
            AS question_set_owner_user_count,
        qs.single_question_set_id,
        qs.single_question_position,
        qs.unambiguous_qset_owner_user_id,
        qs.question_set_id_min,
        qs.question_set_id_max,
        vr.user_question_record_id,
        vr.voter_user_id AS vote_actor_user_id,
        vr.chosen_user_id,
        vr.question_id AS vote_question_id,
        vr.vote_record_created_at,
        (vr.user_question_record_id IS NOT NULL) AS vote_record_exists_flag
    FROM votes_mart.mart_question_piece_record_v2 AS qp
    LEFT JOIN qset_rollup AS qs
      ON qs.question_piece_id = qp.question_piece_id
    LEFT JOIN votes_mart.mart_vote_record_v2 AS vr
      ON vr.question_piece_id = qp.question_piece_id
)
SELECT
    ob.*,
    (ob.question_set_membership_row_count>0)
        AS question_set_membership_observed_flag,
    (ob.question_set_membership_row_count>1)
        AS ambiguous_question_set_mapping_flag,
    CASE
        WHEN ob.question_set_owner_user_count=1
         AND ob.vote_actor_user_id IS NOT NULL
         AND ob.unambiguous_qset_owner_user_id=ob.vote_actor_user_id
            THEN ob.unambiguous_qset_owner_user_id
        WHEN ob.question_set_owner_user_count=1
         AND ob.vote_actor_user_id IS NULL
            THEN ob.unambiguous_qset_owner_user_id
        WHEN ob.question_set_owner_user_count=0
         AND ob.vote_actor_user_id IS NOT NULL
            THEN ob.vote_actor_user_id
        ELSE NULL
    END AS resolved_owner_user_id,
    CASE
        WHEN ob.question_set_owner_user_count=1
         AND ob.vote_actor_user_id IS NOT NULL
         AND ob.unambiguous_qset_owner_user_id=ob.vote_actor_user_id THEN 1
        WHEN ob.question_set_owner_user_count=1
         AND ob.vote_actor_user_id IS NULL THEN 2
        WHEN ob.question_set_owner_user_count=0
         AND ob.vote_actor_user_id IS NOT NULL THEN 3
        WHEN ob.question_set_owner_user_count=1
         AND ob.vote_actor_user_id IS NOT NULL
         AND ob.unambiguous_qset_owner_user_id<>ob.vote_actor_user_id THEN 4
        WHEN ob.question_set_owner_user_count>1 THEN 5
        ELSE 0
    END AS owner_resolution_code
FROM owner_base AS ob;

ALTER TABLE votes_mart.bridge_question_piece_owner_v2
    ADD PRIMARY KEY (question_piece_id),
    ADD INDEX idx_piece_owner_resolved (resolved_owner_user_id),
    ADD INDEX idx_piece_owner_qset (single_question_set_id),
    ADD INDEX idx_piece_owner_vote (user_question_record_id),
    ADD INDEX idx_piece_owner_resolution (owner_resolution_code);


/* ============================================================================
   7. 후보 원행 fact — 가장 큰 테이블은 의도적으로 9개 좁은 컬럼만 저장

   같은 question_piece_id + candidate_user_id가 반복돼도 삭제하지 않는다.
   canonical flag는 비율 계산 시 중복 pair를 한 번만 세기 위한 선택지일 뿐,
   원초 행을 지우는 장치가 아니다.
============================================================================ */

DROP TABLE IF EXISTS votes_mart.mart_question_candidate_exposure_v2;

CREATE TABLE votes_mart.mart_question_candidate_exposure_v2 AS
WITH candidate_ranked AS (
    SELECT
        uc.id AS candidate_exposure_id,
        uc.question_piece_id,
        uc.user_id AS candidate_user_id,
        uc.created_at AS candidate_source_created_at,
        COUNT(*) OVER (
            PARTITION BY uc.question_piece_id, uc.user_id
        ) AS source_candidate_pair_row_count,
        ROW_NUMBER() OVER (
            PARTITION BY uc.question_piece_id, uc.user_id
            ORDER BY uc.created_at, uc.id
        ) AS candidate_pair_occurrence_number
    FROM final.polls_usercandidate AS uc
)
SELECT
    cr.candidate_exposure_id,
    cr.question_piece_id,
    cr.candidate_user_id,
    cr.candidate_source_created_at,
    cr.source_candidate_pair_row_count,
    cr.candidate_pair_occurrence_number,
    (cr.source_candidate_pair_row_count>1) AS duplicate_candidate_pair_flag,
    (cr.candidate_pair_occurrence_number=1) AS canonical_candidate_pair_row_flag,
    (qp.question_piece_id IS NULL) AS orphan_question_piece_flag
FROM candidate_ranked AS cr
LEFT JOIN votes_mart.mart_question_piece_record_v2 AS qp
  ON qp.question_piece_id = cr.question_piece_id;

ALTER TABLE votes_mart.mart_question_candidate_exposure_v2
    ADD PRIMARY KEY (candidate_exposure_id),
    ADD INDEX idx_candidate_piece_user (
        question_piece_id, candidate_user_id, candidate_pair_occurrence_number
    ),
    ADD INDEX idx_candidate_user_time (
        candidate_user_id, candidate_source_created_at
    );


/* ============================================================================
   8. 질문자-후보 distinct pair 관계 bridge

   현재 친구 판정은 3,600만 physical edge를 다시 저장·조인하지 않는다.
   약 27만 개의 실제 owner-candidate pair에 대해서만 두 사용자의 현재
   friends JSON을 직접 확인한다. 친구요청도 해당 pair의 양방향만 집계한다.
============================================================================ */

DROP TABLE IF EXISTS votes_mart.bridge_question_owner_candidate_relation_v2;

CREATE TABLE votes_mart.bridge_question_owner_candidate_relation_v2 AS
WITH pair_seed AS (
    SELECT DISTINCT
        po.resolved_owner_user_id AS owner_user_id,
        c.candidate_user_id
    FROM votes_mart.mart_question_candidate_exposure_v2 AS c
    JOIN votes_mart.bridge_question_piece_owner_v2 AS po
      ON po.question_piece_id = c.question_piece_id
    WHERE po.resolved_owner_user_id IS NOT NULL
      AND c.candidate_user_id IS NOT NULL
), request_pair_rollup AS (
    SELECT
        p.owner_user_id,
        p.candidate_user_id,
        COALESCE(o2c.request_record_count,0)
            AS owner_to_candidate_request_count_all_time,
        COALESCE(o2c.final_accepted_record_count,0)
            AS owner_to_candidate_final_accepted_count,
        COALESCE(o2c.final_pending_record_count,0)
            AS owner_to_candidate_final_pending_count,
        COALESCE(o2c.final_rejected_record_count,0)
            AS owner_to_candidate_final_rejected_count,
        o2c.first_request_created_at_raw
            AS owner_to_candidate_first_request_at,
        o2c.last_request_created_at_raw
            AS owner_to_candidate_last_request_at,
        o2c.first_accepted_effective_at_proxy
            AS owner_to_candidate_first_accepted_at_proxy,
        o2c.last_accepted_effective_at_proxy
            AS owner_to_candidate_last_accepted_at_proxy,

        COALESCE(c2o.request_record_count,0)
            AS candidate_to_owner_request_count_all_time,
        COALESCE(c2o.final_accepted_record_count,0)
            AS candidate_to_owner_final_accepted_count,
        COALESCE(c2o.final_pending_record_count,0)
            AS candidate_to_owner_final_pending_count,
        COALESCE(c2o.final_rejected_record_count,0)
            AS candidate_to_owner_final_rejected_count,
        c2o.first_request_created_at_raw
            AS candidate_to_owner_first_request_at,
        c2o.last_request_created_at_raw
            AS candidate_to_owner_last_request_at,
        c2o.first_accepted_effective_at_proxy
            AS candidate_to_owner_first_accepted_at_proxy,
        c2o.last_accepted_effective_at_proxy
            AS candidate_to_owner_last_accepted_at_proxy
    FROM pair_seed AS p
    LEFT JOIN votes_mart.mart_friend_request_pair_summary_v2 AS o2c
      ON o2c.send_user_id = p.owner_user_id
     AND o2c.receive_user_id = p.candidate_user_id
    LEFT JOIN votes_mart.mart_friend_request_pair_summary_v2 AS c2o
      ON c2o.send_user_id = p.candidate_user_id
     AND c2o.receive_user_id = p.owner_user_id
), pair_with_current_flags AS (
    SELECT
        p.owner_user_id,
        p.candidate_user_id,
        (owner.user_id IS NOT NULL) AS owner_current_profile_match_flag,
        (candidate.user_id IS NOT NULL) AS candidate_current_profile_match_flag,
        CASE
            WHEN owner.user_id IS NULL
              OR owner.current_friend_json_valid<>1 THEN NULL
            ELSE JSON_CONTAINS(
                owner.current_friend_id_list_json,
                JSON_ARRAY(p.candidate_user_id),
                '$'
            )
        END AS owner_lists_candidate_current_friend_flag,
        CASE
            WHEN candidate.user_id IS NULL
              OR candidate.current_friend_json_valid<>1 THEN NULL
            ELSE JSON_CONTAINS(
                candidate.current_friend_id_list_json,
                JSON_ARRAY(p.owner_user_id),
                '$'
            )
        END AS candidate_lists_owner_current_friend_flag,
        CASE
            WHEN owner.current_school_id IS NULL
              OR candidate.current_school_id IS NULL THEN NULL
            ELSE owner.current_school_id=candidate.current_school_id
        END AS same_school_current_flag,
        CASE
            WHEN owner.current_school_id IS NULL
              OR candidate.current_school_id IS NULL
              OR owner.current_grade IS NULL
              OR candidate.current_grade IS NULL THEN NULL
            ELSE owner.current_school_id=candidate.current_school_id
             AND owner.current_grade=candidate.current_grade
        END AS same_school_grade_current_flag,
        CASE
            WHEN owner.current_school_id IS NULL
              OR candidate.current_school_id IS NULL
              OR owner.current_grade IS NULL
              OR candidate.current_grade IS NULL
              OR owner.current_class_num IS NULL
              OR candidate.current_class_num IS NULL THEN NULL
            ELSE owner.current_school_id=candidate.current_school_id
             AND owner.current_grade=candidate.current_grade
             AND owner.current_class_num=candidate.current_class_num
        END AS same_school_grade_class_current_flag,
        CASE
            WHEN owner.current_group_id IS NULL
              OR candidate.current_group_id IS NULL THEN NULL
            ELSE owner.current_group_id=candidate.current_group_id
        END AS same_group_current_flag,
        (p.owner_user_id=p.candidate_user_id) AS self_candidate_flag
    FROM pair_seed AS p
    LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS owner
      ON owner.user_id = p.owner_user_id
    LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS candidate
      ON candidate.user_id = p.candidate_user_id
)
SELECT
    pf.*,
    CASE
        WHEN pf.owner_lists_candidate_current_friend_flag IS NULL
         AND pf.candidate_lists_owner_current_friend_flag IS NULL THEN NULL
        ELSE COALESCE(pf.owner_lists_candidate_current_friend_flag,0)=1
          OR COALESCE(pf.candidate_lists_owner_current_friend_flag,0)=1
    END AS current_friend_any_direction_flag,
    CASE
        WHEN pf.owner_lists_candidate_current_friend_flag IS NULL
          OR pf.candidate_lists_owner_current_friend_flag IS NULL THEN NULL
        ELSE pf.owner_lists_candidate_current_friend_flag=1
         AND pf.candidate_lists_owner_current_friend_flag=1
    END AS current_friend_mutual_flag,
    COALESCE(rr.owner_to_candidate_request_count_all_time,0)
        AS owner_to_candidate_request_count_all_time,
    COALESCE(rr.owner_to_candidate_final_accepted_count,0)
        AS owner_to_candidate_final_accepted_count,
    COALESCE(rr.owner_to_candidate_final_pending_count,0)
        AS owner_to_candidate_final_pending_count,
    COALESCE(rr.owner_to_candidate_final_rejected_count,0)
        AS owner_to_candidate_final_rejected_count,
    rr.owner_to_candidate_first_request_at,
    rr.owner_to_candidate_last_request_at,
    rr.owner_to_candidate_first_accepted_at_proxy,
    rr.owner_to_candidate_last_accepted_at_proxy,
    COALESCE(rr.candidate_to_owner_request_count_all_time,0)
        AS candidate_to_owner_request_count_all_time,
    COALESCE(rr.candidate_to_owner_final_accepted_count,0)
        AS candidate_to_owner_final_accepted_count,
    COALESCE(rr.candidate_to_owner_final_pending_count,0)
        AS candidate_to_owner_final_pending_count,
    COALESCE(rr.candidate_to_owner_final_rejected_count,0)
        AS candidate_to_owner_final_rejected_count,
    rr.candidate_to_owner_first_request_at,
    rr.candidate_to_owner_last_request_at,
    rr.candidate_to_owner_first_accepted_at_proxy,
    rr.candidate_to_owner_last_accepted_at_proxy
FROM pair_with_current_flags AS pf
LEFT JOIN request_pair_rollup AS rr
  ON rr.owner_user_id = pf.owner_user_id
 AND rr.candidate_user_id = pf.candidate_user_id;

ALTER TABLE votes_mart.bridge_question_owner_candidate_relation_v2
    ADD PRIMARY KEY (owner_user_id, candidate_user_id),
    ADD INDEX idx_qrel_candidate_owner (candidate_user_id, owner_user_id),
    ADD INDEX idx_qrel_current_friend (current_friend_any_direction_flag),
    ADD INDEX idx_qrel_same_school (same_school_current_flag),
    ADD INDEX idx_qrel_same_grade (same_school_grade_current_flag),
    ADD INDEX idx_qrel_same_class (same_school_grade_class_current_flag);


/* ============================================================================
   9. 분석 편의 VIEW — 물리 중복 없이 후보 원행 단위로 모든 축을 연결

   current_friend/school/grade/class 비율은 canonical_candidate_pair_row_flag=1,
   resolved_owner_user_id IS NOT NULL 조건을 권장한다. 원초 후보 행 빈도를
   그대로 분석하려면 canonical 조건을 빼면 된다.
============================================================================ */

CREATE VIEW votes_mart.vw_question_candidate_analysis_v2 AS
SELECT
    c.candidate_exposure_id,
    c.question_piece_id,
    c.candidate_user_id,
    c.candidate_source_created_at,
    c.source_candidate_pair_row_count,
    c.candidate_pair_occurrence_number,
    c.duplicate_candidate_pair_flag,
    c.canonical_candidate_pair_row_flag,
    c.orphan_question_piece_flag,

    qp.question_id,
    qp.question_piece_created_at,
    qp.raw_is_voted,
    qp.raw_is_skipped,
    qp.voted_and_skipped_flag,
    q.question_text,
    q.question_created_at,

    po.question_set_membership_row_count,
    po.question_set_id_count,
    po.question_set_owner_user_count,
    po.question_set_membership_observed_flag,
    po.ambiguous_question_set_mapping_flag,
    po.single_question_set_id AS question_set_id,
    po.single_question_position AS question_position,
    po.question_set_id_min,
    po.question_set_id_max,
    po.unambiguous_qset_owner_user_id,
    po.resolved_owner_user_id,
    po.owner_resolution_code,

    qs.question_set_status,
    qs.question_set_created_at,
    qs.question_set_opening_time,
    qs.opening_delay_seconds AS question_set_opening_delay_seconds,

    po.user_question_record_id,
    po.vote_record_exists_flag,
    po.vote_actor_user_id,
    po.chosen_user_id,
    po.vote_question_id,
    po.vote_record_created_at,
    vr.vote_status_current,
    vr.ping_has_read_current,
    vr.ping_answer_status_current,
    vr.ping_answer_status_updated_at,
    vr.ping_report_count_current,
    vr.ping_opened_times_current,
    (
        po.user_question_record_id IS NOT NULL
        AND po.chosen_user_id=c.candidate_user_id
    ) AS selected_candidate_raw_row_flag,
    (
        po.user_question_record_id IS NOT NULL
        AND po.chosen_user_id=c.candidate_user_id
        AND c.canonical_candidate_pair_row_flag=1
    ) AS selected_candidate_canonical_pair_flag,

    owner.signup_at AS owner_signup_at,
    owner.current_group_id AS owner_current_group_id,
    owner.current_school_id AS owner_current_school_id,
    owner.current_school_type AS owner_current_school_type,
    owner.current_grade AS owner_current_grade,
    owner.current_class_num AS owner_current_class_num,
    candidate.signup_at AS candidate_signup_at,
    candidate.current_group_id AS candidate_current_group_id,
    candidate.current_school_id AS candidate_current_school_id,
    candidate.current_school_type AS candidate_current_school_type,
    candidate.current_grade AS candidate_current_grade,
    candidate.current_class_num AS candidate_current_class_num,

    rel.owner_current_profile_match_flag,
    rel.candidate_current_profile_match_flag,
    rel.owner_lists_candidate_current_friend_flag,
    rel.candidate_lists_owner_current_friend_flag,
    rel.current_friend_any_direction_flag,
    rel.current_friend_mutual_flag,
    rel.same_school_current_flag,
    rel.same_school_grade_current_flag,
    rel.same_school_grade_class_current_flag,
    rel.same_group_current_flag,
    rel.self_candidate_flag,

    rel.owner_to_candidate_request_count_all_time,
    rel.owner_to_candidate_final_accepted_count,
    rel.owner_to_candidate_final_pending_count,
    rel.owner_to_candidate_final_rejected_count,
    rel.owner_to_candidate_first_request_at,
    rel.owner_to_candidate_last_request_at,
    rel.owner_to_candidate_first_accepted_at_proxy,
    rel.owner_to_candidate_last_accepted_at_proxy,
    rel.candidate_to_owner_request_count_all_time,
    rel.candidate_to_owner_final_accepted_count,
    rel.candidate_to_owner_final_pending_count,
    rel.candidate_to_owner_final_rejected_count,
    rel.candidate_to_owner_first_request_at,
    rel.candidate_to_owner_last_request_at,
    rel.candidate_to_owner_first_accepted_at_proxy,
    rel.candidate_to_owner_last_accepted_at_proxy,

    CASE
        WHEN rel.owner_user_id IS NULL THEN NULL
        WHEN rel.owner_to_candidate_first_request_at IS NULL THEN 0
        WHEN rel.owner_to_candidate_first_request_at<=c.candidate_source_created_at
            THEN 1 ELSE 0
    END AS owner_to_candidate_request_before_candidate_proxy_flag,
    CASE
        WHEN rel.owner_user_id IS NULL THEN NULL
        WHEN rel.candidate_to_owner_first_request_at IS NULL THEN 0
        WHEN rel.candidate_to_owner_first_request_at<=c.candidate_source_created_at
            THEN 1 ELSE 0
    END AS candidate_to_owner_request_before_candidate_proxy_flag,
    CASE
        WHEN rel.owner_user_id IS NULL THEN NULL
        WHEN rel.owner_to_candidate_first_accepted_at_proxy IS NULL THEN 0
        WHEN rel.owner_to_candidate_first_accepted_at_proxy
             <=c.candidate_source_created_at THEN 1 ELSE 0
    END AS owner_to_candidate_accepted_before_candidate_proxy_flag,
    CASE
        WHEN rel.owner_user_id IS NULL THEN NULL
        WHEN rel.candidate_to_owner_first_accepted_at_proxy IS NULL THEN 0
        WHEN rel.candidate_to_owner_first_accepted_at_proxy
             <=c.candidate_source_created_at THEN 1 ELSE 0
    END AS candidate_to_owner_accepted_before_candidate_proxy_flag
FROM votes_mart.mart_question_candidate_exposure_v2 AS c
LEFT JOIN votes_mart.mart_question_piece_record_v2 AS qp
  ON qp.question_piece_id = c.question_piece_id
LEFT JOIN votes_mart.dim_question_v2 AS q
  ON q.question_id = qp.question_id
LEFT JOIN votes_mart.bridge_question_piece_owner_v2 AS po
  ON po.question_piece_id = c.question_piece_id
LEFT JOIN votes_mart.mart_question_set_record_v2 AS qs
  ON qs.question_set_id = po.single_question_set_id
LEFT JOIN votes_mart.mart_vote_record_v2 AS vr
  ON vr.user_question_record_id = po.user_question_record_id
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS owner
  ON owner.user_id = po.resolved_owner_user_id
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS candidate
  ON candidate.user_id = c.candidate_user_id
LEFT JOIN votes_mart.bridge_question_owner_candidate_relation_v2 AS rel
  ON rel.owner_user_id = po.resolved_owner_user_id
 AND rel.candidate_user_id = c.candidate_user_id;


/* ============================================================================
   10. 실행 후 QA — 결과를 캡처·보관
============================================================================ */

/* QA-1. 원초 표와 원장 행 수 및 고유 id가 정확히 일치해야 한다. */
SELECT
    source_name,
    source_rows,
    mart_rows,
    source_rows-mart_rows AS row_difference,
    duplicate_primary_key_rows
FROM (
    SELECT
        'polls_question' AS source_name,
        (SELECT COUNT(*) FROM final.polls_question) AS source_rows,
        COUNT(*) AS mart_rows,
        COUNT(*)-COUNT(DISTINCT question_id) AS duplicate_primary_key_rows
    FROM votes_mart.dim_question_v2

    UNION ALL
    SELECT
        'polls_questionpiece',
        (SELECT COUNT(*) FROM final.polls_questionpiece),
        COUNT(*),
        COUNT(*)-COUNT(DISTINCT question_piece_id)
    FROM votes_mart.mart_question_piece_record_v2

    UNION ALL
    SELECT
        'polls_questionset',
        (SELECT COUNT(*) FROM final.polls_questionset),
        COUNT(*),
        COUNT(*)-COUNT(DISTINCT question_set_id)
    FROM votes_mart.mart_question_set_record_v2

    UNION ALL
    SELECT
        'accounts_userquestionrecord',
        (SELECT COUNT(*) FROM final.accounts_userquestionrecord),
        COUNT(*),
        COUNT(*)-COUNT(DISTINCT user_question_record_id)
    FROM votes_mart.mart_vote_record_v2

    UNION ALL
    SELECT
        'polls_usercandidate',
        (SELECT COUNT(*) FROM final.polls_usercandidate),
        COUNT(*),
        COUNT(*)-COUNT(DISTINCT candidate_exposure_id)
    FROM votes_mart.mart_question_candidate_exposure_v2
) AS qa
ORDER BY source_name;


/* QA-2. 유효한 qset JSON의 원소 수와 펼친 bridge 행 수가 일치해야 한다. */
SELECT
    COUNT(*) AS question_set_count,
    SUM(piece_list_array_valid_flag=1) AS valid_array_question_set_count,
    SUM(piece_list_array_valid_flag=0) AS invalid_or_nonarray_question_set_count,
    SUM(CASE WHEN piece_list_array_valid_flag=1
             THEN piece_list_length ELSE 0 END) AS expected_position_rows,
    (SELECT COUNT(*)
     FROM votes_mart.bridge_question_set_piece_v2) AS actual_position_rows,
    (SELECT SUM(question_piece_exists_flag=0)
     FROM votes_mart.bridge_question_set_piece_v2)
        AS referenced_piece_missing_from_current_source_rows
FROM votes_mart.mart_question_set_record_v2;


/* QA-3. 후보 원초 id 보존과 canonical pair reconciliation. */
SELECT
    COUNT(*) AS candidate_source_rows_preserved,
    COUNT(DISTINCT candidate_exposure_id) AS distinct_candidate_source_ids,
    COUNT(*)-COUNT(DISTINCT candidate_exposure_id)
        AS duplicate_candidate_source_id_rows,
    COUNT(DISTINCT question_piece_id, candidate_user_id)
        AS distinct_piece_candidate_pairs,
    SUM(canonical_candidate_pair_row_flag=1) AS canonical_pair_rows,
    SUM(duplicate_candidate_pair_flag=1) AS rows_belonging_to_duplicate_pairs,
    SUM(orphan_question_piece_flag=1) AS orphan_question_piece_rows
FROM votes_mart.mart_question_candidate_exposure_v2;


/* QA-4. VIEW 조인이 후보 행을 늘리거나 줄이지 않았는지 확인한다. */
SELECT
    (SELECT COUNT(*)
     FROM votes_mart.mart_question_candidate_exposure_v2) AS fact_rows,
    COUNT(*) AS analysis_view_rows,
    COUNT(*)-(SELECT COUNT(*)
              FROM votes_mart.mart_question_candidate_exposure_v2)
        AS view_minus_fact_rows,
    SUM(selected_candidate_canonical_pair_flag=1)
        AS selected_candidate_canonical_rows,
    (SELECT COUNT(*)
     FROM votes_mart.mart_vote_record_v2
     WHERE chosen_user_id IS NOT NULL) AS uqr_with_chosen_user_rows
FROM votes_mart.vw_question_candidate_analysis_v2;


/* QA-5. 현재 스냅샷 기준 후보-질문자 관계율. 과거 관계로 해석 금지. */
SELECT
    COUNT(*) AS canonical_candidate_pairs_with_resolved_owner,
    SUM(current_friend_any_direction_flag IS NOT NULL)
        AS current_friend_observed_pairs,
    ROUND(
        100*AVG(CASE WHEN current_friend_any_direction_flag IS NOT NULL
                     THEN current_friend_any_direction_flag END), 2
    ) AS current_friend_match_pct,
    SUM(same_school_current_flag IS NOT NULL) AS school_observed_pairs,
    ROUND(
        100*AVG(CASE WHEN same_school_current_flag IS NOT NULL
                     THEN same_school_current_flag END), 2
    ) AS same_school_pct,
    ROUND(
        100*AVG(CASE WHEN same_school_grade_current_flag IS NOT NULL
                     THEN same_school_grade_current_flag END), 2
    ) AS same_school_grade_pct,
    ROUND(
        100*AVG(CASE WHEN same_school_grade_class_current_flag IS NOT NULL
                     THEN same_school_grade_class_current_flag END), 2
    ) AS same_school_grade_class_pct,
    ROUND(
        100*AVG(
            owner_to_candidate_accepted_before_candidate_proxy_flag=1
            OR candidate_to_owner_accepted_before_candidate_proxy_flag=1
        ), 2
    ) AS accepted_request_before_candidate_proxy_pct
FROM votes_mart.vw_question_candidate_analysis_v2
WHERE canonical_candidate_pair_row_flag=1
  AND resolved_owner_user_id IS NOT NULL;


/* QA-6. 실제 디스크 용량. 예상치가 아니라 실행 DB의 data/index 크기다. */
SELECT
    table_name,
    table_rows,
    ROUND(data_length/1024/1024,1) AS data_mb,
    ROUND(index_length/1024/1024,1) AS index_mb,
    ROUND((data_length+index_length)/1024/1024,1) AS total_mb
FROM information_schema.tables
WHERE table_schema='votes_mart'
  AND table_name IN (
      'dim_question_v2',
      'mart_question_piece_record_v2',
      'mart_question_set_record_v2',
      'bridge_question_set_piece_v2',
      'mart_vote_record_v2',
      'bridge_question_piece_owner_v2',
      'mart_question_candidate_exposure_v2',
      'bridge_question_owner_candidate_relation_v2'
  )
ORDER BY total_mb DESC;


/* --------------------------------------------------------------------------
   권장 분석 예시

   -- 현재 친구 일치율
   SELECT AVG(current_friend_any_direction_flag)
   FROM votes_mart.vw_question_candidate_analysis_v2
   WHERE canonical_candidate_pair_row_flag=1
     AND resolved_owner_user_id IS NOT NULL
     AND current_friend_any_direction_flag IS NOT NULL;

   -- 현재 학교·학년·반 일치율
   SELECT
       AVG(same_school_current_flag),
       AVG(same_school_grade_current_flag),
       AVG(same_school_grade_class_current_flag)
   FROM votes_mart.vw_question_candidate_analysis_v2
   WHERE canonical_candidate_pair_row_flag=1
     AND resolved_owner_user_id IS NOT NULL;

   -- 특정 후보가 실제 선택됐는지
   SELECT question_id, question_text,
          SUM(selected_candidate_canonical_pair_flag) AS selected_count
   FROM votes_mart.vw_question_candidate_analysis_v2
   WHERE canonical_candidate_pair_row_flag=1
   GROUP BY question_id, question_text;
---------------------------------------------------------------------------- */
