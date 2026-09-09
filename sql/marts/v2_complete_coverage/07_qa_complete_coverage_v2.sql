/* ============================================================================
   07. 전체 25개 원초 테이블 커버리지 및 핵심 grain QA

   PASS 기준
   - 1:1 보존 대상은 원천 행수 = 보존 객체 행수
   - 원천 PK/이벤트 키 기반 상세 마트는 중복 0
   - 요약 마트 합계는 상세 마트 합계와 일치

   이 파일은 결과를 votes_mart.mart_build_*_qa_v2에 저장한다.
============================================================================ */

CREATE DATABASE IF NOT EXISTS votes_mart;

DROP TABLE IF EXISTS votes_mart.mart_build_source_coverage_qa_v2;

CREATE TABLE votes_mart.mart_build_source_coverage_qa_v2 AS
SELECT
    x.source_table,
    x.preserved_object,
    x.source_row_count,
    x.preserved_row_count,
    CAST(x.source_row_count AS SIGNED)-CAST(x.preserved_row_count AS SIGNED) AS difference,
    CASE WHEN x.source_row_count=x.preserved_row_count THEN 'PASS' ELSE 'FAIL' END AS qa_status
FROM (
    SELECT 'accounts_attendance' source_table, 'mart_attendance_record_v2' preserved_object,
           (SELECT COUNT(*) FROM final.accounts_attendance) source_row_count,
           (SELECT COUNT(*) FROM votes_mart.mart_attendance_record_v2) preserved_row_count
    UNION ALL SELECT 'accounts_blockrecord','mart_safety_event_v2/USER_BLOCK',
           (SELECT COUNT(*) FROM final.accounts_blockrecord),
           (SELECT COUNT(*) FROM votes_mart.mart_safety_event_v2 WHERE source_table='accounts_blockrecord')
    UNION ALL SELECT 'accounts_failpaymenthistory','mart_value_event_v2/accounts_failpaymenthistory',
           (SELECT COUNT(*) FROM final.accounts_failpaymenthistory),
           (SELECT COUNT(*) FROM votes_mart.mart_value_event_v2 WHERE source_table='accounts_failpaymenthistory')
    UNION ALL SELECT 'accounts_friendrequest','mart_friend_request_event_v2',
           (SELECT COUNT(*) FROM final.accounts_friendrequest),
           (SELECT COUNT(*) FROM votes_mart.mart_friend_request_event_v2)
    UNION ALL SELECT 'accounts_group','dim_group_current_v2',
           (SELECT COUNT(*) FROM final.accounts_group),
           (SELECT COUNT(*) FROM votes_mart.dim_group_current_v2)
    UNION ALL SELECT 'accounts_nearbyschool','bridge_school_neighbor_v2',
           (SELECT COUNT(*) FROM final.accounts_nearbyschool),
           (SELECT COUNT(*) FROM votes_mart.bridge_school_neighbor_v2)
    UNION ALL SELECT 'accounts_paymenthistory','mart_value_event_v2/accounts_paymenthistory',
           (SELECT COUNT(*) FROM final.accounts_paymenthistory),
           (SELECT COUNT(*) FROM votes_mart.mart_value_event_v2 WHERE source_table='accounts_paymenthistory')
    UNION ALL SELECT 'accounts_user_contacts','mart_user_contact_record_v2',
           (SELECT COUNT(*) FROM final.accounts_user_contacts),
           (SELECT COUNT(*) FROM votes_mart.mart_user_contact_record_v2)
    UNION ALL SELECT 'accounts_pointhistory','mart_value_event_v2/accounts_pointhistory',
           (SELECT COUNT(*) FROM final.accounts_pointhistory),
           (SELECT COUNT(*) FROM votes_mart.mart_value_event_v2 WHERE source_table='accounts_pointhistory')
    UNION ALL SELECT 'accounts_school','dim_school_current_v2',
           (SELECT COUNT(*) FROM final.accounts_school),
           (SELECT COUNT(*) FROM votes_mart.dim_school_current_v2)
    UNION ALL SELECT 'accounts_timelinereport','mart_safety_event_v2/TIMELINE_REPORT',
           (SELECT COUNT(*) FROM final.accounts_timelinereport),
           (SELECT COUNT(*) FROM votes_mart.mart_safety_event_v2 WHERE source_table='accounts_timelinereport')
    UNION ALL SELECT 'accounts_user','mart_user_acquisition_profile_v2',
           (SELECT COUNT(*) FROM final.accounts_user),
           (SELECT COUNT(*) FROM votes_mart.mart_user_acquisition_profile_v2)
    UNION ALL SELECT 'accounts_userquestionrecord','mart_vote_record_v2',
           (SELECT COUNT(*) FROM final.accounts_userquestionrecord),
           (SELECT COUNT(*) FROM votes_mart.mart_vote_record_v2)
    UNION ALL SELECT 'accounts_userwithdraw','mart_lifecycle_event_v2/WITHDRAW',
           (SELECT COUNT(*) FROM final.accounts_userwithdraw),
           (SELECT COUNT(*) FROM votes_mart.mart_lifecycle_event_v2 WHERE source_table='accounts_userwithdraw')
    UNION ALL SELECT 'event_receipts','mart_promo_event_receipt_v2',
           (SELECT COUNT(*) FROM final.event_receipts),
           (SELECT COUNT(*) FROM votes_mart.mart_promo_event_receipt_v2)
    UNION ALL SELECT 'events','dim_promo_event_v2',
           (SELECT COUNT(*) FROM final.events),
           (SELECT COUNT(*) FROM votes_mart.dim_promo_event_v2)
    UNION ALL SELECT 'polls_question','dim_question_v2',
           (SELECT COUNT(*) FROM final.polls_question),
           (SELECT COUNT(*) FROM votes_mart.dim_question_v2)
    UNION ALL SELECT 'polls_questionpiece','mart_question_piece_record_v2',
           (SELECT COUNT(*) FROM final.polls_questionpiece),
           (SELECT COUNT(*) FROM votes_mart.mart_question_piece_record_v2)
    UNION ALL SELECT 'polls_questionreport','mart_safety_event_v2/QUESTION_FEEDBACK_OR_REPORT',
           (SELECT COUNT(*) FROM final.polls_questionreport),
           (SELECT COUNT(*) FROM votes_mart.mart_safety_event_v2 WHERE source_table='polls_questionreport')
    UNION ALL SELECT 'polls_questionset','mart_question_set_record_v2',
           (SELECT COUNT(*) FROM final.polls_questionset),
           (SELECT COUNT(*) FROM votes_mart.mart_question_set_record_v2)
    UNION ALL SELECT 'polls_usercandidate','mart_question_candidate_exposure_v2',
           (SELECT COUNT(*) FROM final.polls_usercandidate),
           (SELECT COUNT(*) FROM votes_mart.mart_question_candidate_exposure_v2)
    UNION ALL SELECT 'hackle_properties','bridge_hackle_session_property_raw_v2',
           (SELECT COUNT(*) FROM final.hackle_properties),
           (SELECT COUNT(*) FROM votes_mart.bridge_hackle_session_property_raw_v2)
    UNION ALL SELECT 'device_properties','bridge_hackle_device_property_raw_v2',
           (SELECT COUNT(*) FROM final.device_properties),
           (SELECT COUNT(*) FROM votes_mart.bridge_hackle_device_property_raw_v2)
    UNION ALL SELECT 'hackle_events','fact_hackle_event_24d_v2',
           (SELECT COUNT(*) FROM final.hackle_events),
           (SELECT COUNT(*) FROM votes_mart.fact_hackle_event_24d_v2)
    UNION ALL SELECT 'user_properties','dim_hackle_user_property_v2',
           (SELECT COUNT(*) FROM final.user_properties),
           (SELECT COUNT(*) FROM votes_mart.dim_hackle_user_property_v2)
) AS x;

ALTER TABLE votes_mart.mart_build_source_coverage_qa_v2
    ADD PRIMARY KEY (source_table);


DROP TABLE IF EXISTS votes_mart.mart_build_integrity_qa_v2;

CREATE TABLE votes_mart.mart_build_integrity_qa_v2 AS
SELECT
    q.test_name,
    q.actual_value,
    q.expected_value,
    CAST(q.actual_value AS SIGNED)-CAST(q.expected_value AS SIGNED) AS difference,
    CASE WHEN q.actual_value=q.expected_value THEN 'PASS' ELSE 'FAIL' END AS qa_status,
    q.meaning
FROM (
    SELECT
        'friend_request_duplicate_request_id_rows' AS test_name,
        COUNT(*)-COUNT(DISTINCT request_id) AS actual_value,
        0 AS expected_value,
        '친구요청 상세 grain 중복 0' AS meaning
    FROM votes_mart.mart_friend_request_event_v2

    UNION ALL
    SELECT
        'friend_request_status_partition_difference',
        COUNT(*)-(SUM(final_status_code='A')+SUM(final_status_code='P')+SUM(final_status_code='R')),
        0,
        'A/P/R 최종상태 합이 전체 요청과 일치'
    FROM votes_mart.mart_friend_request_event_v2

    UNION ALL
    SELECT
        'user_profile_duplicate_user_id_rows',
        COUNT(*)-COUNT(DISTINCT user_id), 0,
        '전체 사용자 profile grain 중복 0'
    FROM votes_mart.mart_user_acquisition_profile_v2

    UNION ALL
    SELECT
        'question_candidate_duplicate_source_id_rows',
        COUNT(*)-COUNT(DISTINCT candidate_exposure_id), 0,
        '후보 원초 id 1건당 정확히 1행'
    FROM votes_mart.mart_question_candidate_exposure_v2

    UNION ALL
    SELECT
        'question_candidate_canonical_pair_reconciliation',
        SUM(candidate_pair_occurrence_number=1),
        COUNT(DISTINCT question_piece_id, candidate_user_id),
        '후보 pair마다 대표 표시가 정확히 1행'
    FROM votes_mart.mart_question_candidate_exposure_v2

    UNION ALL
    SELECT
        'hackle_duplicate_event_id_rows',
        COUNT(*)-COUNT(DISTINCT event_id), 0,
        'Hackle 이벤트 1건당 정확히 1행'
    FROM votes_mart.fact_hackle_event_24d_v2

    UNION ALL
    SELECT
        'hackle_ambiguous_session_forced_user_assignments',
        SUM(user_conflict_flag=1 AND resolved_hackle_user_sk IS NOT NULL), 0,
        '복수 사용자 세션을 임의 사용자에게 귀속하지 않음'
    FROM votes_mart.dim_hackle_session_resolved_v2

    UNION ALL
    SELECT
        'withdraw_rows_with_forced_user_id',
        SUM(lifecycle_event_type='WITHDRAW' AND service_user_id IS NOT NULL), 0,
        'user_id가 없는 탈퇴 행을 억지 연결하지 않음'
    FROM votes_mart.mart_lifecycle_event_v2

    UNION ALL
    SELECT
        'activity_duplicate_user_date_rows',
        COUNT(*)-COUNT(DISTINCT user_id, activity_date), 0,
        '사용자×날짜 grain 중복 0'
    FROM votes_mart.mart_user_activity_daily_v2

    UNION ALL
    SELECT
        'timeline_report_uqr_fk_mismatch_rows',
        COALESCE(SUM(NOT (m.user_question_record_id <=> tr.user_question_record_id)),0), 0,
        '타임라인 신고의 원천 user_question_record_id를 값까지 보존'
    FROM final.accounts_timelinereport AS tr
    JOIN votes_mart.mart_safety_event_v2 AS m
      ON m.source_table='accounts_timelinereport'
     AND m.source_row_id=tr.id

    UNION ALL
    SELECT
        'user_viral_rate_decimal_definition_mismatches',
        3-COALESCE(SUM(
            data_type='decimal'
            AND numeric_precision=18
            AND numeric_scale=10
            AND is_nullable='YES'
        ),0), 0,
        '사용자 바이럴 비율 3개가 DECIMAL(18,10)으로 보존됨'
    FROM information_schema.columns
    WHERE table_schema='votes_mart'
      AND table_name='mart_user_viral_profile_v2'
      AND column_name IN (
          'sent_eventual_acceptance_rate_including_pending',
          'sent_eventual_decision_acceptance_rate',
          'received_eventual_acceptance_rate_including_pending'
      )

    UNION ALL
    SELECT
        'school_viral_rate_decimal_definition_mismatches',
        3-COALESCE(SUM(
            data_type='decimal'
            AND numeric_precision=18
            AND numeric_scale=10
            AND is_nullable='YES'
        ),0), 0,
        '학교×날짜 바이럴 비율 3개가 DECIMAL(18,10)으로 보존됨'
    FROM information_schema.columns
    WHERE table_schema='votes_mart'
      AND table_name='mart_school_viral_daily_v2'
      AND column_name IN (
          'sent_eventual_acceptance_rate_including_pending',
          'sent_eventual_decision_acceptance_rate',
          'received_eventual_acceptance_rate_including_pending'
      )

    UNION ALL
    SELECT
        'user_viral_rate_out_of_range_rows',
        COALESCE(SUM(
            (sent_eventual_acceptance_rate_including_pending IS NOT NULL
             AND NOT sent_eventual_acceptance_rate_including_pending BETWEEN 0 AND 1)
            OR
            (sent_eventual_decision_acceptance_rate IS NOT NULL
             AND NOT sent_eventual_decision_acceptance_rate BETWEEN 0 AND 1)
            OR
            (received_eventual_acceptance_rate_including_pending IS NOT NULL
             AND NOT received_eventual_acceptance_rate_including_pending BETWEEN 0 AND 1)
        ),0), 0,
        '사용자 바이럴 비율은 NULL 또는 0~1 범위'
    FROM votes_mart.mart_user_viral_profile_v2

    UNION ALL
    SELECT
        'school_viral_rate_out_of_range_rows',
        COALESCE(SUM(
            (sent_eventual_acceptance_rate_including_pending IS NOT NULL
             AND NOT sent_eventual_acceptance_rate_including_pending BETWEEN 0 AND 1)
            OR
            (sent_eventual_decision_acceptance_rate IS NOT NULL
             AND NOT sent_eventual_decision_acceptance_rate BETWEEN 0 AND 1)
            OR
            (received_eventual_acceptance_rate_including_pending IS NOT NULL
             AND NOT received_eventual_acceptance_rate_including_pending BETWEEN 0 AND 1)
        ),0), 0,
        '학교×날짜 바이럴 비율은 NULL 또는 0~1 범위'
    FROM votes_mart.mart_school_viral_daily_v2

    UNION ALL
    SELECT
        'user_viral_rate_value_mismatch_rows',
        COALESCE(SUM(
            NOT (
                sent_eventual_acceptance_rate_including_pending <=> CAST(
                    CASE WHEN sent_request_count=0 THEN NULL
                         ELSE CAST(sent_final_accepted_count AS DECIMAL(20,6))
                              / NULLIF(CAST(sent_request_count AS DECIMAL(20,6)),0)
                    END AS DECIMAL(18,10)
                )
            )
            OR NOT (
                sent_eventual_decision_acceptance_rate <=> CAST(
                    CASE WHEN sent_final_accepted_count+sent_final_rejected_count=0 THEN NULL
                         ELSE CAST(sent_final_accepted_count AS DECIMAL(20,6))
                              / NULLIF(
                                  CAST(sent_final_accepted_count+sent_final_rejected_count AS DECIMAL(20,6)),
                                  0
                                )
                    END AS DECIMAL(18,10)
                )
            )
            OR NOT (
                received_eventual_acceptance_rate_including_pending <=> CAST(
                    CASE WHEN received_request_count=0 THEN NULL
                         ELSE CAST(received_final_accepted_count AS DECIMAL(20,6))
                              / NULLIF(CAST(received_request_count AS DECIMAL(20,6)),0)
                    END AS DECIMAL(18,10)
                )
            )
        ),0), 0,
        '사용자 바이럴 비율 3개가 보존된 분자·분모의 재계산값과 정확히 일치'
    FROM votes_mart.mart_user_viral_profile_v2

    UNION ALL
    SELECT
        'school_viral_rate_value_mismatch_rows',
        COALESCE(SUM(
            NOT (
                sent_eventual_acceptance_rate_including_pending <=> CAST(
                    CASE WHEN sent_request_created_count=0 THEN NULL
                         ELSE CAST(sent_created_final_accepted_count AS DECIMAL(20,6))
                              / NULLIF(CAST(sent_request_created_count AS DECIMAL(20,6)),0)
                    END AS DECIMAL(18,10)
                )
            )
            OR NOT (
                sent_eventual_decision_acceptance_rate <=> CAST(
                    CASE WHEN sent_created_final_accepted_count+sent_created_final_rejected_count=0 THEN NULL
                         ELSE CAST(sent_created_final_accepted_count AS DECIMAL(20,6))
                              / NULLIF(
                                  CAST(sent_created_final_accepted_count+sent_created_final_rejected_count AS DECIMAL(20,6)),
                                  0
                                )
                    END AS DECIMAL(18,10)
                )
            )
            OR NOT (
                received_eventual_acceptance_rate_including_pending <=> CAST(
                    CASE WHEN received_request_created_count=0 THEN NULL
                         ELSE CAST(received_created_final_accepted_count AS DECIMAL(20,6))
                              / NULLIF(CAST(received_request_created_count AS DECIMAL(20,6)),0)
                    END AS DECIMAL(18,10)
                )
            )
        ),0), 0,
        '학교×날짜 바이럴 비율 3개가 보존된 분자·분모의 재계산값과 정확히 일치'
    FROM votes_mart.mart_school_viral_daily_v2

    UNION ALL
    SELECT
        'cumulative_hackle_identified_event_reconciliation',
        ABS(
            (SELECT COALESCE(SUM(hackle_24d_event_count),0)
             FROM votes_mart.mart_user_cumulative_state_1y_v2)
            -
            (SELECT COALESCE(SUM(v.visit_event_count_30m),0)
             FROM votes_mart.dim_hackle_visit_30m_v2 AS v
             JOIN votes_mart.dim_hackle_session_resolved_v2 AS hs
               ON hs.session_sk=v.original_session_sk
             JOIN votes_mart.dim_hackle_user_resolved_v2 AS hu
               ON hu.hackle_user_sk=hs.resolved_hackle_user_sk
             WHERE hu.service_user_id IS NOT NULL)
        ), 0,
        '누적 상태의 식별 사용자 Hackle 이벤트 합이 30분 방문 사전집계와 일치'

    UNION ALL
    SELECT
        'cumulative_hackle_identified_visit_reconciliation',
        ABS(
            (SELECT COALESCE(SUM(hackle_24d_visit_count),0)
             FROM votes_mart.mart_user_cumulative_state_1y_v2)
            -
            (SELECT COUNT(*)
             FROM votes_mart.dim_hackle_visit_30m_v2 AS v
             JOIN votes_mart.dim_hackle_session_resolved_v2 AS hs
               ON hs.session_sk=v.original_session_sk
             JOIN votes_mart.dim_hackle_user_resolved_v2 AS hu
               ON hu.hackle_user_sk=hs.resolved_hackle_user_sk
             WHERE hu.service_user_id IS NOT NULL)
        ), 0,
        '누적 상태의 식별 사용자 Hackle 방문 합이 30분 방문 원행수와 일치'

    UNION ALL
    SELECT
        'source_coverage_failed_tables',
        SUM(qa_status='FAIL'), 0,
        '25개 원초표 모두 지정된 보존 객체와 행수 일치'
    FROM votes_mart.mart_build_source_coverage_qa_v2
) AS q;

ALTER TABLE votes_mart.mart_build_integrity_qa_v2
    ADD PRIMARY KEY (test_name);


/* 실행 후 가장 먼저 볼 두 출력 */
SELECT *
FROM votes_mart.mart_build_source_coverage_qa_v2
ORDER BY (qa_status='FAIL') DESC, source_table;

SELECT *
FROM votes_mart.mart_build_integrity_qa_v2
ORDER BY (qa_status='FAIL') DESC, test_name;

/* 핵심 범위·규모 확인 */
SELECT
    'friend_request' AS mart_area,
    COUNT(*) AS row_count,
    COUNT(DISTINCT send_user_id) AS actor_count,
    MIN(request_created_at_raw) AS min_at,
    MAX(request_created_at_raw) AS max_at
FROM votes_mart.mart_friend_request_event_v2
UNION ALL
SELECT
    'question_candidate', COUNT(*), COUNT(DISTINCT candidate_user_id),
    MIN(candidate_source_created_at), MAX(candidate_source_created_at)
FROM votes_mart.mart_question_candidate_exposure_v2
UNION ALL
SELECT
    'hackle_event_24d', SUM(v.visit_event_count_30m), COUNT(DISTINCT hu.service_user_id),
    MIN(v.visit_start_at_30m), MAX(v.visit_end_at_30m)
FROM votes_mart.dim_hackle_visit_30m_v2 AS v
JOIN votes_mart.dim_hackle_session_resolved_v2 AS hs
  ON hs.session_sk=v.original_session_sk
LEFT JOIN votes_mart.dim_hackle_user_resolved_v2 AS hu
  ON hu.hackle_user_sk=hs.resolved_hackle_user_sk
UNION ALL
SELECT
    'activity_sparse_user_day', COUNT(*), COUNT(DISTINCT user_id),
    MIN(activity_date), MAX(activity_date)
FROM votes_mart.mart_user_activity_daily_v2;

/*
   mysql CLI는 --execute 모드에서 SQL 오류가 나면 비정상 종료한다. QA FAIL이
   하나라도 있으면 임시 PK 중복 오류를 의도적으로 발생시켜, 눈으로 결과를
   놓쳐도 Stage 3가 성공으로 끝나지 않게 한다. 저장 프로시저나 영구 객체는
   만들지 않는다.
*/
SET @v2_final_qa_fail_count =
    (SELECT COUNT(*) FROM votes_mart.mart_build_source_coverage_qa_v2 WHERE qa_status='FAIL')
    +
    (SELECT COUNT(*) FROM votes_mart.mart_build_integrity_qa_v2 WHERE qa_status='FAIL');

SELECT @v2_final_qa_fail_count AS final_qa_fail_count,
       CASE WHEN @v2_final_qa_fail_count=0 THEN 'PASS' ELSE 'FAIL' END AS final_qa_status;

DROP TEMPORARY TABLE IF EXISTS tmp_v2_final_qa_guard;
CREATE TEMPORARY TABLE tmp_v2_final_qa_guard (
    guard_id TINYINT NOT NULL PRIMARY KEY
);
INSERT INTO tmp_v2_final_qa_guard (guard_id) VALUES (1);
INSERT INTO tmp_v2_final_qa_guard (guard_id)
SELECT 1
WHERE @v2_final_qa_fail_count > 0;
DROP TEMPORARY TABLE tmp_v2_final_qa_guard;
