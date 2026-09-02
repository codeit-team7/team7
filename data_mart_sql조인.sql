-- ==============================================================================
-- [SQL VERSION] 익명 SNS 데이터 마트 구축 및 테이블 조인 쿼리문
-- ==============================================================================

-- ==============================================================================
-- 1. 포인트 및 결제 통합 마트 (mart_point_payment_event)
-- 조인 테이블: accounts_pointhistory, accounts_paymenthistory, accounts_failpaymenthistory,
--              hackle_events, hackle_properties, accounts_user, accounts_userquestionrecord
-- ==============================================================================
CREATE OR REPLACE TABLE `mart_point_payment_event` AS

-- (1) 유저 세션 1:1 매핑 (다대다 조인 폭발 방지)
WITH valid_sessions AS (
    SELECT 
        session_id,
        CAST(user_id AS INT64) AS mapped_user_id
    FROM `hackle_properties`
    WHERE REGEXP_CONTAINS(user_id, r'^\d+$')
    GROUP BY session_id, user_id
    QUALIFY COUNT(*) OVER(PARTITION BY session_id) = 1
),

-- (2) 포인트 획득 및 소비 내역 (DB 원장 + 유저상태 + Ping컨텍스트 조인)
point_events AS (
    SELECT
        CONCAT('PH_', ph.id)                  AS value_event_id,
        CASE WHEN ph.delta_point > 0 THEN 'POINT_EARN' ELSE 'POINT_SPEND' END AS event_type,
        ph.created_at                         AS event_at,
        ph.user_id                            AS user_id,
        CAST(NULL AS STRING)                  AS session_id,
        u.ban_status                          AS identity_status,
        ph.delta_point                        AS point_delta,
        ABS(ph.delta_point)                   AS point_amount_abs,
        CAST(NULL AS STRING)                  AS product_id,
        CAST(NULL AS STRING)                  AS phone_type,
        CAST(NULL AS BOOL)                    AS is_success,
        ph.user_question_record_id            AS user_question_record_id,
        uqr.question_id                       AS ping_question_id,
        uqr.has_read                          AS ping_has_read,
        uqr.answer_status                     AS ping_answer_status,
        'accounts_pointhistory'               AS source_table,
        FALSE                                 AS is_outlier
    FROM `accounts_pointhistory` ph
    LEFT JOIN `accounts_user` u 
        ON ph.user_id = u.id
    LEFT JOIN `accounts_userquestionrecord` uqr 
        ON ph.user_question_record_id = uqr.id
),

-- (3) 결제 성공 원장 (DB 원장 + 유저상태 조인)
payment_success AS (
    SELECT
        CONCAT('PAY_', pay.id)                AS value_event_id,
        'PAYMENT_SUCCESS'                     AS event_type,
        pay.created_at                        AS event_at,
        pay.user_id                           AS user_id,
        CAST(NULL AS STRING)                  AS session_id,
        u.ban_status                          AS identity_status,
        CAST(NULL AS INT64)                   AS point_delta,
        CAST(NULL AS INT64)                   AS point_amount_abs,
        pay.productId                         AS product_id,
        pay.phone_type                        AS phone_type,
        TRUE                                  AS is_success,
        CAST(NULL AS INT64)                   AS user_question_record_id,
        CAST(NULL AS INT64)                   AS ping_question_id,
        CAST(NULL AS INT64)                   AS ping_has_read,
        CAST(NULL AS STRING)                  AS ping_answer_status,
        'accounts_paymenthistory'             AS source_table,
        FALSE                                 AS is_outlier
    FROM `accounts_paymenthistory` pay
    LEFT JOIN `accounts_user` u 
        ON pay.user_id = u.id
),

-- (4) 결제 실패 원장 (DB 원장 + 유저상태 조인)
payment_fail AS (
    SELECT
        CONCAT('FAIL_', fail.id)              AS value_event_id,
        'PAYMENT_FAIL'                        AS event_type,
        fail.created_at                       AS event_at,
        fail.user_id                          AS user_id,
        CAST(NULL AS STRING)                  AS session_id,
        u.ban_status                          AS identity_status,
        CAST(NULL AS INT64)                   AS point_delta,
        CAST(NULL AS INT64)                   AS point_amount_abs,
        fail.productId                        AS product_id,
        fail.phone_type                       AS phone_type,
        FALSE                                 AS is_success,
        CAST(NULL AS INT64)                   AS user_question_record_id,
        CAST(NULL AS INT64)                   AS ping_question_id,
        CAST(NULL AS INT64)                   AS ping_has_read,
        CAST(NULL AS STRING)                  AS ping_answer_status,
        'accounts_failpaymenthistory'         AS source_table,
        FALSE                                 AS is_outlier
    FROM `accounts_failpaymenthistory` fail
    LEFT JOIN `accounts_user` u 
        ON fail.user_id = u.id
),

-- (5) Hackle 상점/결제 클릭 이벤트 (클라이언트 로그 + 세션조인 + 유저상태 조인)
hackle_shop_events AS (
    SELECT
        CONCAT('HE_', he.event_id)            AS value_event_id,
        CASE 
            WHEN he.event_key = 'view_shop' THEN 'SHOP_VIEW'
            WHEN he.event_key = 'click_purchase' THEN 'PRODUCT_CLICK'
            WHEN he.event_key = 'complete_purchase' THEN 'PURCHASE_COMPLETE_EVENT'
        END                                   AS event_type,
        he.event_datetime                     AS event_at,
        vs.mapped_user_id                     AS user_id,
        he.session_id                         AS session_id,
        u.ban_status                          AS identity_status,
        CAST(NULL AS INT64)                   AS point_delta,
        CAST(NULL AS INT64)                   AS point_amount_abs,
        CAST(NULL AS STRING)                  AS product_id,
        CAST(NULL AS STRING)                  AS phone_type,
        CAST(NULL AS BOOL)                    AS is_success,
        CAST(NULL AS INT64)                   AS user_question_record_id,
        CAST(NULL AS INT64)                   AS ping_question_id,
        CAST(NULL AS INT64)                   AS ping_has_read,
        CAST(NULL AS STRING)                  AS ping_answer_status,
        'hackle_events'                       AS source_table,
        FALSE                                 AS is_outlier
    FROM `hackle_events` he
    LEFT JOIN valid_sessions vs 
        ON he.session_id = vs.session_id
    LEFT JOIN `accounts_user` u 
        ON vs.mapped_user_id = u.id
    WHERE he.event_key IN ('view_shop', 'click_purchase', 'complete_purchase')
)

-- 최종 통합 UNION ALL
SELECT * FROM point_events
UNION ALL
SELECT * FROM payment_success
UNION ALL
SELECT * FROM payment_fail
UNION ALL
SELECT * FROM hackle_shop_events;


-- ==============================================================================
-- 2. 안전 및 신고 통합 마트 (mart_safety_event)
-- 조인 테이블: polls_questionreport, accounts_blockrecord, accounts_timelinereport,
--              accounts_userquestionrecord, accounts_user, accounts_group, accounts_school
-- ==============================================================================
CREATE OR REPLACE TABLE `mart_safety_event` AS

-- (1) 유저 소속 학교 및 지역(시·도) 매핑 테이블 조인
WITH user_school_region AS (
    SELECT
        u.id                                  AS user_id,
        g.school_id                           AS school_id,
        s.address                             AS school_address,
        REGEXP_EXTRACT(s.address, r'^([^\s]+)') AS region
    FROM `accounts_user` u
    LEFT JOIN `accounts_group` g  ON u.group_id = g.id
    LEFT JOIN `accounts_school` s ON g.school_id = s.id
),

-- (2) 질문 신고 (질문문구 + 행위자 학교/지역 조인)
q_reports AS (
    SELECT
        CONCAT('QR_', qr.id)                  AS safety_event_id,
        'QUESTION_REPORT'                     AS safety_event_type,
        qr.created_at                         AS event_at,
        qr.user_id                            AS actor_user_id,
        CAST(NULL AS INT64)                   AS target_user_id,
        qr.question_id                        AS question_id,
        q.question                            AS question_text,
        CAST(NULL AS INT64)                   AS user_question_record_id,
        qr.reason                             AS reason_raw,
        CASE
            WHEN REGEXP_CONTAINS(LOWER(qr.reason), r'욕설|비방|협박|위험|폭력|죽|살') THEN '안전·유해'
            WHEN REGEXP_CONTAINS(LOWER(qr.reason), r'음란|성적|신체|외모|혐오|민감|야한') THEN '불쾌·민감'
            WHEN REGEXP_CONTAINS(LOWER(qr.reason), r'이상|의미없|스팸|광고|반복|관련없') THEN '질문 품질'
            WHEN REGEXP_CONTAINS(LOWER(qr.reason), r'모르는|친하지|불쾌|싫어|관심없') THEN '사용자 선호 불일치'
            WHEN REGEXP_CONTAINS(LOWER(qr.reason), r'좋아|재밌|재미있|좋음') THEN '긍정 피드백'
            ELSE '기타'
        END                                   AS reason_category,
        usr.school_id                         AS actor_school_id,
        usr.region                            AS actor_region,
        CAST(NULL AS INT64)                   AS target_school_id,
        CAST(NULL AS STRING)                  AS target_region,
        'polls_questionreport'                AS source_table
    FROM `polls_questionreport` qr
    LEFT JOIN `polls_question` q 
        ON qr.question_id = q.id
    LEFT JOIN user_school_region usr 
        ON qr.user_id = usr.user_id
),

-- (3) 유저 차단 (차단자/피차단자 학교 및 지역 양방향 조인)
user_blocks AS (
    SELECT
        CONCAT('BL_', bl.id)                  AS safety_event_id,
        'USER_BLOCK'                          AS safety_event_type,
        bl.created_at                         AS event_at,
        bl.user_id                            AS actor_user_id,
        bl.block_user_id                      AS target_user_id,
        CAST(NULL AS INT64)                   AS question_id,
        CAST(NULL AS STRING)                  AS question_text,
        CAST(NULL AS INT64)                   AS user_question_record_id,
        bl.reason                             AS reason_raw,
        CASE
            WHEN REGEXP_CONTAINS(LOWER(bl.reason), r'욕설|비방|협박|위험|폭력|죽|살') THEN '안전·유해'
            WHEN REGEXP_CONTAINS(LOWER(bl.reason), r'음란|성적|신체|외모|혐오|민감|야한') THEN '불쾌·민감'
            WHEN REGEXP_CONTAINS(LOWER(bl.reason), r'이상|의미없|스팸|광고|반복|관련없') THEN '질문 품질'
            WHEN REGEXP_CONTAINS(LOWER(bl.reason), r'모르는|친하지|불쾌|싫어|관심없') THEN '사용자 선호 불일치'
            ELSE '기타'
        END                                   AS reason_category,
        usr_actor.school_id                   AS actor_school_id,
        usr_actor.region                      AS actor_region,
        usr_target.school_id                  AS target_school_id,
        usr_target.region                     AS target_region,
        'accounts_blockrecord'                AS source_table
    FROM `accounts_blockrecord` bl
    LEFT JOIN user_school_region usr_actor 
        ON bl.user_id = usr_actor.user_id
    LEFT JOIN user_school_region usr_target 
        ON bl.block_user_id = usr_target.user_id
),

-- (4) 타임라인 Ping 신고
timeline_reports AS (
    SELECT
        CONCAT('TR_', tr.id)                  AS safety_event_id,
        'TIMELINE_REPORT'                     AS safety_event_type,
        tr.created_at                         AS event_at,
        tr.user_id                            AS actor_user_id,
        tr.reported_user_id                   AS target_user_id,
        CAST(NULL AS INT64)                   AS question_id,
        CAST(NULL AS STRING)                  AS question_text,
        tr.user_question_record_id            AS user_question_record_id,
        CAST(NULL AS STRING)                  AS reason_raw,
        '불쾌·민감'                           AS reason_category,
        usr_actor.school_id                   AS actor_school_id,
        usr_actor.region                      AS actor_region,
        usr_target.school_id                  AS target_school_id,
        usr_target.region                     AS target_region,
        'accounts_timelinereport'             AS source_table
    FROM `accounts_timelinereport` tr
    LEFT JOIN user_school_region usr_actor  ON tr.user_id = usr_actor.user_id
    LEFT JOIN user_school_region usr_target ON tr.reported_user_id = usr_target.user_id
)

SELECT * FROM q_reports
UNION ALL
SELECT * FROM user_blocks
UNION ALL
SELECT * FROM timeline_reports;


-- ==============================================================================
-- 3. 질문 성과 및 10문항 완주 마트 (mart_question_performance)
-- 조인 테이블: polls_questionpiece, polls_question, polls_usercandidate,
--              accounts_userquestionrecord, polls_questionreport
-- ==============================================================================
CREATE OR REPLACE TABLE `mart_question_performance` AS
WITH piece_stats AS (
    SELECT
        qp.question_id,
        COUNT(*)                               AS expose_count,
        COUNTIF(qp.is_voted = 1)               AS voted_count,
        COUNTIF(qp.is_skipped = 1)             AS skipped_count,
        SAFE_DIVIDE(COUNTIF(qp.is_voted = 1), COUNT(*))   AS vote_rate,
        SAFE_DIVIDE(COUNTIF(qp.is_skipped = 1), COUNT(*)) AS skip_rate
    FROM `polls_questionpiece` qp
    GROUP BY qp.question_id
),
cand_stats AS (
    SELECT
        qp.question_id,
        COUNT(uc.id)                           AS candidate_exposed_count
    FROM `polls_questionpiece` qp
    LEFT JOIN `polls_usercandidate` uc ON qp.id = uc.question_piece_id
    GROUP BY qp.question_id
),
ping_stats AS (
    SELECT
        uqr.question_id,
        COUNT(*)                               AS ping_sent_count,
        COUNTIF(uqr.has_read = 1)              AS ping_read_count,
        SAFE_DIVIDE(COUNTIF(uqr.has_read = 1), COUNT(*)) AS ping_read_rate,
        COUNTIF(uqr.answer_status != 'N')      AS answer_count,
        COUNTIF(uqr.answer_status = 'P')       AS public_answer_count,
        COUNTIF(uqr.answer_status = 'S')       AS secret_answer_count
    FROM `accounts_userquestionrecord` uqr
    GROUP BY uqr.question_id
),
report_stats AS (
    SELECT
        question_id,
        COUNT(*) AS report_count
    FROM `polls_questionreport`
    GROUP BY question_id
)
SELECT
    q.id AS question_id,
    q.question AS question_text,
    COALESCE(ps.expose_count, 0)               AS expose_count,
    COALESCE(ps.voted_count, 0)                AS voted_count,
    COALESCE(ps.skipped_count, 0)              AS skipped_count,
    COALESCE(ps.vote_rate, 0.0)                AS vote_rate,
    COALESCE(ps.skip_rate, 0.0)                AS skip_rate,
    COALESCE(cs.candidate_exposed_count, 0)    AS candidate_exposed_count,
    COALESCE(pings.ping_sent_count, 0)         AS ping_sent_count,
    COALESCE(pings.ping_read_count, 0)         AS ping_read_count,
    COALESCE(pings.ping_read_rate, 0.0)        AS ping_read_rate,
    COALESCE(pings.answer_count, 0)            AS answer_count,
    COALESCE(pings.public_answer_count, 0)     AS public_answer_count,
    COALESCE(pings.secret_answer_count, 0)     AS secret_answer_count,
    COALESCE(rs.report_count, 0)               AS report_count,
    SAFE_DIVIDE(COALESCE(rs.report_count, 0), NULLIF(ps.expose_count, 0)) AS report_rate
FROM `polls_question` q
LEFT JOIN piece_stats ps   ON q.id = ps.question_id
LEFT JOIN cand_stats cs    ON q.id = cs.question_id
LEFT JOIN ping_stats pings ON q.id = pings.question_id
LEFT JOIN report_stats rs  ON q.id = rs.question_id;
