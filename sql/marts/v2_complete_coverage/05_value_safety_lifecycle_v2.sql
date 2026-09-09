/* ============================================================================
   05. 가치(포인트/결제/프로모션) + 안전 + 가입/탈퇴 생애주기 v2

   목적
   - 서로 다른 모집단과 의미를 한 숫자로 합산하지 않되, 원초 행은 빠짐없이
     분석용 fact/dim에 보존한다.
   - source_table + source_row_id를 항상 남겨 원천까지 추적할 수 있게 한다.
   - 기존 mart_* 테이블은 덮어쓰지 않는다.

   원천 커버리지
   - final.accounts_pointhistory
   - final.accounts_paymenthistory
   - final.accounts_failpaymenthistory
   - final.events
   - final.event_receipts
   - final.polls_questionreport
   - final.accounts_blockrecord
   - final.accounts_timelinereport
   - final.accounts_userquestionrecord (누적 report_count > 0 스냅샷)
   - final.accounts_userwithdraw
   - final.accounts_user (가입 이벤트)
   - votes_mart.mart_hackle_event_enriched_24d_v2 (상점 UX 이벤트)
============================================================================ */

CREATE DATABASE IF NOT EXISTS votes_mart;

/* --------------------------------------------------------------------------
   05-A. 프로모션 이벤트 정의 3건을 독립 dim으로 전부 보존
---------------------------------------------------------------------------- */
DROP TABLE IF EXISTS votes_mart.dim_promo_event_v2;

CREATE TABLE votes_mart.dim_promo_event_v2 AS
SELECT
    e.id AS promo_event_id,
    e.title AS promo_event_title,
    e.plus_point AS promised_plus_point,
    e.event_type AS promo_event_type,
    e.is_expired AS current_is_expired,
    e.created_at AS promo_event_created_at,
    'CURRENT_EVENT_DEFINITION_SNAPSHOT' AS definition_time_scope
FROM final.events AS e;

ALTER TABLE votes_mart.dim_promo_event_v2
    ADD PRIMARY KEY (promo_event_id);


/* --------------------------------------------------------------------------
   05-B. 프로모션 참여·지급 원행 309건
---------------------------------------------------------------------------- */
DROP TABLE IF EXISTS votes_mart.mart_promo_event_receipt_v2;

CREATE TABLE votes_mart.mart_promo_event_receipt_v2 AS
SELECT
    r.id AS promo_receipt_id,
    r.created_at AS receipt_created_at,
    r.event_id AS promo_event_id,
    r.user_id,
    r.plus_point AS actually_granted_point,
    (d.promo_event_id IS NULL) AS orphan_event_definition_flag,
    (p.user_id IS NULL) AS orphan_user_flag,
    'GLOBAL_DB_EVENT_RECEIPT' AS source_scope
FROM final.event_receipts AS r
LEFT JOIN votes_mart.dim_promo_event_v2 AS d
  ON d.promo_event_id = r.event_id
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS p
  ON p.user_id = r.user_id;

ALTER TABLE votes_mart.mart_promo_event_receipt_v2
    ADD PRIMARY KEY (promo_receipt_id),
    ADD INDEX idx_promo_receipt_user_time (user_id, receipt_created_at),
    ADD INDEX idx_promo_receipt_event (promo_event_id);


/* --------------------------------------------------------------------------
   05-C. 가치 이벤트 통합 원장

   주의:
   - POINT_*는 질문/포인트 원천의 상위 10개 학교 범위일 가능성이 높다.
   - DB PAYMENT_SUCCESS/FAIL은 전역 DB 거래 기록이다.
   - HACKLE_*은 2023-07-18~2023-08-10 클라이언트 UX 로그다.
   - DB PAYMENT_SUCCESS와 HACKLE_PURCHASE_COMPLETE는 같은 결제의 양쪽 기록일
     수 있으므로 단순 합산 금지. source_scope/source_system으로 분리한다.
---------------------------------------------------------------------------- */
DROP TABLE IF EXISTS votes_mart.mart_value_event_v2;

CREATE TABLE votes_mart.mart_value_event_v2 (
    source_table                ENUM(
        'accounts_pointhistory','accounts_paymenthistory',
        'accounts_failpaymenthistory','event_receipts','hackle_events'
    ) NOT NULL,
    source_row_id               VARCHAR(255) NOT NULL,
    source_system               ENUM('DB','HACKLE') NOT NULL,
    source_scope                ENUM(
        'TOP10_SCHOOL_DB','GLOBAL_DB_PAYMENT',
        'GLOBAL_DB_PAYMENT_FAIL_INCOMPLETE_PERIOD',
        'GLOBAL_DB_EVENT_RECEIPT','HACKLE_24D'
    ) NOT NULL,
    event_type                  ENUM(
        'POINT_EARN','POINT_SPEND','POINT_ZERO_DELTA',
        'PAYMENT_SUCCESS','PAYMENT_FAIL','PROMO_POINT_RECEIPT',
        'HACKLE_SHOP_VIEW','HACKLE_PRODUCT_CLICK',
        'HACKLE_PURCHASE_COMPLETE'
    ) NOT NULL,
    event_at_raw                DATETIME NULL,
    service_user_id             BIGINT NULL,
    analytics_visit_session_id  VARCHAR(350) NULL,
    original_session_id         VARCHAR(255) NULL,
    point_delta                 INT NULL,
    point_amount_abs            INT NULL,
    product_id                  VARCHAR(255) NULL,
    phone_type                  VARCHAR(20) NULL,
    payment_success_flag        TINYINT NULL,
    user_question_record_id     BIGINT NULL,
    ping_question_id            BIGINT NULL,
    ping_has_read_current       TINYINT NULL,
    ping_answer_status_current  VARCHAR(10) NULL,
    promo_event_id              BIGINT NULL,
    actually_granted_point      INT NULL,
    item_name_raw               VARCHAR(255) NULL,
    page_name_raw               VARCHAR(255) NULL,
    user_match_flag             TINYINT NOT NULL,
    time_zone_officially_known_flag TINYINT NOT NULL DEFAULT 0,
    PRIMARY KEY (source_table, source_row_id),
    KEY idx_value_user_time (service_user_id, event_at_raw),
    KEY idx_value_type_time (event_type, event_at_raw)
);

INSERT INTO votes_mart.mart_value_event_v2
SELECT
    'accounts_pointhistory', CAST(ph.id AS CHAR),
    'DB', 'TOP10_SCHOOL_DB',
    CASE WHEN ph.delta_point > 0 THEN 'POINT_EARN'
         WHEN ph.delta_point < 0 THEN 'POINT_SPEND'
         ELSE 'POINT_ZERO_DELTA' END,
    ph.created_at, ph.user_id,
    NULL, NULL,
    ph.delta_point, ABS(ph.delta_point),
    NULL, NULL, NULL,
    ph.user_question_record_id,
    uqr.question_id, uqr.has_read, uqr.answer_status,
    NULL, NULL,
    NULL, NULL,
    (p.user_id IS NOT NULL), 0
FROM final.accounts_pointhistory AS ph
LEFT JOIN final.accounts_userquestionrecord AS uqr
  ON uqr.id = ph.user_question_record_id
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS p
  ON p.user_id = ph.user_id

UNION ALL

SELECT
    'accounts_paymenthistory', CAST(pay.id AS CHAR),
    'DB', 'GLOBAL_DB_PAYMENT',
    'PAYMENT_SUCCESS', pay.created_at, pay.user_id,
    NULL, NULL,
    NULL, NULL,
    pay.productId, pay.phone_type, 1,
    NULL, NULL, NULL, NULL,
    NULL, NULL,
    NULL, NULL,
    (p.user_id IS NOT NULL), 0
FROM final.accounts_paymenthistory AS pay
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS p
  ON p.user_id = pay.user_id

UNION ALL

SELECT
    'accounts_failpaymenthistory', CAST(fail.id AS CHAR),
    'DB', 'GLOBAL_DB_PAYMENT_FAIL_INCOMPLETE_PERIOD',
    'PAYMENT_FAIL', fail.created_at, fail.user_id,
    NULL, NULL,
    NULL, NULL,
    fail.productId, fail.phone_type, 0,
    NULL, NULL, NULL, NULL,
    NULL, NULL,
    NULL, NULL,
    (p.user_id IS NOT NULL), 0
FROM final.accounts_failpaymenthistory AS fail
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS p
  ON p.user_id = fail.user_id

UNION ALL

SELECT
    'event_receipts', CAST(pr.promo_receipt_id AS CHAR),
    'DB', 'GLOBAL_DB_EVENT_RECEIPT',
    'PROMO_POINT_RECEIPT', pr.receipt_created_at, pr.user_id,
    NULL, NULL,
    pr.actually_granted_point, ABS(pr.actually_granted_point),
    NULL, NULL, NULL,
    NULL, NULL, NULL, NULL,
    pr.promo_event_id, pr.actually_granted_point,
    NULL, NULL,
    (p.user_id IS NOT NULL), 0
FROM votes_mart.mart_promo_event_receipt_v2 AS pr
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS p
  ON p.user_id = pr.user_id

UNION ALL

SELECT
    'hackle_events', CAST(h.event_id AS CHAR),
    'HACKLE', 'HACKLE_24D',
    CASE hek.attribute_value_raw
        WHEN 'view_shop' THEN 'HACKLE_SHOP_VIEW'
        WHEN 'click_purchase' THEN 'HACKLE_PRODUCT_CLICK'
        WHEN 'complete_purchase' THEN 'HACKLE_PURCHASE_COMPLETE'
    END,
    h.event_datetime_raw, hu.service_user_id,
    LOWER(HEX(hva.derived_visit_id_30m)), hs.original_session_id,
    NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL,
    NULL, NULL,
    NULL, NULL,
    hitem.attribute_value_raw, hpage.attribute_value_raw,
    (hu.service_user_id IS NOT NULL), 0
FROM votes_mart.fact_hackle_event_24d_v2 AS h
JOIN votes_mart.bridge_hackle_event_visit_assignment_v2 AS hva
  ON hva.event_sk=h.event_sk
JOIN votes_mart.dim_hackle_session_resolved_v2 AS hs
  ON hs.session_sk=h.original_session_sk
LEFT JOIN votes_mart.dim_hackle_user_resolved_v2 AS hu
  ON hu.hackle_user_sk=hs.resolved_hackle_user_sk
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS hek
  ON hek.text_attribute_sk=h.event_key_attribute_sk
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS hitem
  ON hitem.text_attribute_sk=h.item_name_attribute_sk
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS hpage
  ON hpage.text_attribute_sk=h.page_name_attribute_sk
WHERE hek.attribute_value_raw IN ('view_shop', 'click_purchase', 'complete_purchase');


/* --------------------------------------------------------------------------
   05-D. 안전·품질 피드백 통합 원장

   PING_REPORTED는 개별 신고 이벤트가 아니라 UQR의 현재 누적 report_count가
   양수인 레코드 스냅샷이다. 그래서 row_weight=1과 report_count_weight를 분리한다.
---------------------------------------------------------------------------- */
DROP TABLE IF EXISTS votes_mart.mart_safety_event_v2;

CREATE TABLE votes_mart.mart_safety_event_v2 (
    source_table                ENUM(
        'polls_questionreport','accounts_blockrecord',
        'accounts_timelinereport','accounts_userquestionrecord'
    ) NOT NULL,
    source_row_id               BIGINT NOT NULL,
    record_type                 VARCHAR(40) NOT NULL,
    event_at_raw                DATETIME NULL,
    actor_user_id               BIGINT NULL,
    target_user_id              BIGINT NULL,
    question_id                 BIGINT NULL,
    user_question_record_id     BIGINT NULL,
    reason_raw                  LONGTEXT NULL,
    source_row_weight           INT NOT NULL DEFAULT 1,
    report_count_weight         INT NULL,
    is_true_event_time_flag     TINYINT NOT NULL,
    actor_user_match_flag       TINYINT NOT NULL,
    target_user_match_flag      TINYINT NULL,
    denominator_available_flag  TINYINT NOT NULL DEFAULT 0,
    PRIMARY KEY (source_table, source_row_id),
    KEY idx_safety_actor_time (actor_user_id, event_at_raw),
    KEY idx_safety_target_time (target_user_id, event_at_raw),
    KEY idx_safety_question (question_id),
    KEY idx_safety_type (record_type)
);

INSERT INTO votes_mart.mart_safety_event_v2
SELECT
    'polls_questionreport', qr.id, 'QUESTION_FEEDBACK_OR_REPORT',
    qr.created_at, qr.user_id, NULL,
    qr.question_id, NULL,
    qr.reason, 1, 1, 1,
    (actor.user_id IS NOT NULL), NULL, 0
FROM final.polls_questionreport AS qr
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS actor
  ON actor.user_id = qr.user_id

UNION ALL

SELECT
    'accounts_blockrecord', br.id, 'USER_BLOCK',
    br.created_at, br.user_id, br.block_user_id,
    NULL, NULL,
    br.reason, 1, 1, 1,
    (actor.user_id IS NOT NULL), (target.user_id IS NOT NULL), 0
FROM final.accounts_blockrecord AS br
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS actor
  ON actor.user_id = br.user_id
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS target
  ON target.user_id = br.block_user_id

UNION ALL

SELECT
    'accounts_timelinereport', tr.id, 'TIMELINE_REPORT',
    tr.created_at, tr.user_id, tr.reported_user_id,
    NULL, tr.user_question_record_id,
    tr.reason, 1, 1, 1,
    (actor.user_id IS NOT NULL), (target.user_id IS NOT NULL), 0
FROM final.accounts_timelinereport AS tr
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS actor
  ON actor.user_id = tr.user_id
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS target
  ON target.user_id = tr.reported_user_id

UNION ALL

SELECT
    'accounts_userquestionrecord', uqr.id, 'PING_REPORT_COUNT_SNAPSHOT',
    uqr.created_at, uqr.user_id, uqr.chosen_user_id,
    uqr.question_id, uqr.id,
    NULL, 1, uqr.report_count, 0,
    (actor.user_id IS NOT NULL), (target.user_id IS NOT NULL), 0
FROM final.accounts_userquestionrecord AS uqr
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS actor
  ON actor.user_id = uqr.user_id
LEFT JOIN votes_mart.mart_user_acquisition_profile_v2 AS target
  ON target.user_id = uqr.chosen_user_id
WHERE uqr.report_count > 0;


/* --------------------------------------------------------------------------
   05-E. 가입/탈퇴 생애주기 원장

   accounts_userwithdraw에는 user_id가 원천부터 없다. 탈퇴 행을 가입자에게
   시간 근접 등으로 억지 연결하지 않고 NULL로 보존한다.
---------------------------------------------------------------------------- */
DROP TABLE IF EXISTS votes_mart.mart_lifecycle_event_v2;

CREATE TABLE votes_mart.mart_lifecycle_event_v2 (
    source_table              ENUM('accounts_user','accounts_userwithdraw') NOT NULL,
    source_row_id             BIGINT NOT NULL,
    lifecycle_event_type      VARCHAR(20) NOT NULL,
    event_at_raw              DATETIME NULL,
    service_user_id           BIGINT NULL,
    reason_raw                VARCHAR(255) NULL,
    user_link_available_flag  TINYINT NOT NULL,
    PRIMARY KEY (source_table, source_row_id),
    KEY idx_lifecycle_time (lifecycle_event_type, event_at_raw),
    KEY idx_lifecycle_user (service_user_id, event_at_raw)
);

INSERT INTO votes_mart.mart_lifecycle_event_v2
SELECT
    'accounts_user', p.user_id, 'SIGNUP', p.signup_at,
    p.user_id, NULL, 1
FROM votes_mart.mart_user_acquisition_profile_v2 AS p

UNION ALL

SELECT
    'accounts_userwithdraw', w.id, 'WITHDRAW', w.created_at,
    NULL, w.reason, 0
FROM final.accounts_userwithdraw AS w;

/* 편의 뷰: 사용자와 연결할 수 없는 탈퇴만 명확히 분리 */
CREATE OR REPLACE VIEW votes_mart.vw_withdrawal_event_unlinked_v2 AS
SELECT *
FROM votes_mart.mart_lifecycle_event_v2
WHERE lifecycle_event_type='WITHDRAW';
