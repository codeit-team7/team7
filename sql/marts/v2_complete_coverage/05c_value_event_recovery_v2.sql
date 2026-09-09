/* ============================================================================
   05-C recovery. 가치 이벤트 통합 원장 재구축

   전제
   - 중단된 05-A/05-B에서 프로모션 dim/receipt가 이미 완성되어 있다.
   - 실행 중이던 단일 대형 INSERT는 08d가 안전하게 식별해 중지한다.

   복구 전략
   - 적재 중에는 PK만 유지하고 다섯 원천을 별도 INSERT로 적재한다.
   - Hackle은 EVENT_KEY 차원의 세 선택값에서 fact 인덱스로 진입한다.
   - 적재가 끝난 뒤 조회용 보조 인덱스를 추가한다.
============================================================================ */

CREATE DATABASE IF NOT EXISTS votes_mart;

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
    PRIMARY KEY (source_table, source_row_id)
);

SET @value_event_loaded_cumulative = 0;

INSERT INTO votes_mart.mart_value_event_v2 (
    source_table,source_row_id,source_system,source_scope,event_type,
    event_at_raw,service_user_id,analytics_visit_session_id,original_session_id,
    point_delta,point_amount_abs,product_id,phone_type,payment_success_flag,
    user_question_record_id,ping_question_id,ping_has_read_current,
    ping_answer_status_current,promo_event_id,actually_granted_point,
    item_name_raw,page_name_raw,user_match_flag,time_zone_officially_known_flag
)
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
  ON p.user_id = ph.user_id;

SET @value_event_branch_rows = ROW_COUNT();
SET @value_event_loaded_cumulative =
    @value_event_loaded_cumulative + @value_event_branch_rows;
SELECT
    'accounts_pointhistory' AS loaded_branch,
    @value_event_branch_rows AS branch_row_count,
    @value_event_loaded_cumulative AS cumulative_row_count,
    2338918 AS expected_cumulative_row_count,
    IF(@value_event_loaded_cumulative=2338918,'MATCH','DIFF') AS expected_count_status;

INSERT INTO votes_mart.mart_value_event_v2 (
    source_table,source_row_id,source_system,source_scope,event_type,
    event_at_raw,service_user_id,analytics_visit_session_id,original_session_id,
    point_delta,point_amount_abs,product_id,phone_type,payment_success_flag,
    user_question_record_id,ping_question_id,ping_has_read_current,
    ping_answer_status_current,promo_event_id,actually_granted_point,
    item_name_raw,page_name_raw,user_match_flag,time_zone_officially_known_flag
)
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
  ON p.user_id = pay.user_id;

SET @value_event_branch_rows = ROW_COUNT();
SET @value_event_loaded_cumulative =
    @value_event_loaded_cumulative + @value_event_branch_rows;
SELECT
    'accounts_paymenthistory' AS loaded_branch,
    @value_event_branch_rows AS branch_row_count,
    @value_event_loaded_cumulative AS cumulative_row_count,
    2434058 AS expected_cumulative_row_count,
    IF(@value_event_loaded_cumulative=2434058,'MATCH','DIFF') AS expected_count_status;

INSERT INTO votes_mart.mart_value_event_v2 (
    source_table,source_row_id,source_system,source_scope,event_type,
    event_at_raw,service_user_id,analytics_visit_session_id,original_session_id,
    point_delta,point_amount_abs,product_id,phone_type,payment_success_flag,
    user_question_record_id,ping_question_id,ping_has_read_current,
    ping_answer_status_current,promo_event_id,actually_granted_point,
    item_name_raw,page_name_raw,user_match_flag,time_zone_officially_known_flag
)
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
  ON p.user_id = fail.user_id;

SET @value_event_branch_rows = ROW_COUNT();
SET @value_event_loaded_cumulative =
    @value_event_loaded_cumulative + @value_event_branch_rows;
SELECT
    'accounts_failpaymenthistory' AS loaded_branch,
    @value_event_branch_rows AS branch_row_count,
    @value_event_loaded_cumulative AS cumulative_row_count,
    2434221 AS expected_cumulative_row_count,
    IF(@value_event_loaded_cumulative=2434221,'MATCH','DIFF') AS expected_count_status;

INSERT INTO votes_mart.mart_value_event_v2 (
    source_table,source_row_id,source_system,source_scope,event_type,
    event_at_raw,service_user_id,analytics_visit_session_id,original_session_id,
    point_delta,point_amount_abs,product_id,phone_type,payment_success_flag,
    user_question_record_id,ping_question_id,ping_has_read_current,
    ping_answer_status_current,promo_event_id,actually_granted_point,
    item_name_raw,page_name_raw,user_match_flag,time_zone_officially_known_flag
)
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
  ON p.user_id = pr.user_id;

SET @value_event_branch_rows = ROW_COUNT();
SET @value_event_loaded_cumulative =
    @value_event_loaded_cumulative + @value_event_branch_rows;
SELECT
    'event_receipts' AS loaded_branch,
    @value_event_branch_rows AS branch_row_count,
    @value_event_loaded_cumulative AS cumulative_row_count,
    2434530 AS expected_cumulative_row_count,
    IF(@value_event_loaded_cumulative=2434530,'MATCH','DIFF') AS expected_count_status;

INSERT INTO votes_mart.mart_value_event_v2 (
    source_table,source_row_id,source_system,source_scope,event_type,
    event_at_raw,service_user_id,analytics_visit_session_id,original_session_id,
    point_delta,point_amount_abs,product_id,phone_type,payment_success_flag,
    user_question_record_id,ping_question_id,ping_has_read_current,
    ping_answer_status_current,promo_event_id,actually_granted_point,
    item_name_raw,page_name_raw,user_match_flag,time_zone_officially_known_flag
)
SELECT STRAIGHT_JOIN
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
    NULL, NULL, NULL, NULL,
    NULL, NULL,
    hitem.attribute_value_raw, hpage.attribute_value_raw,
    (hu.service_user_id IS NOT NULL), 0
FROM votes_mart.dim_hackle_event_text_attribute_v2 AS hek
    FORCE INDEX (idx_het_type_value)
JOIN votes_mart.fact_hackle_event_24d_v2 AS h
    FORCE INDEX (idx_hackle_fact_event_key)
  ON h.event_key_attribute_sk = hek.text_attribute_sk
JOIN votes_mart.bridge_hackle_event_visit_assignment_v2 AS hva
  ON hva.event_sk = h.event_sk
JOIN votes_mart.dim_hackle_session_resolved_v2 AS hs
  ON hs.session_sk = h.original_session_sk
LEFT JOIN votes_mart.dim_hackle_user_resolved_v2 AS hu
  ON hu.hackle_user_sk = hs.resolved_hackle_user_sk
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS hitem
  ON hitem.text_attribute_sk = h.item_name_attribute_sk
 AND hitem.attribute_type = 'ITEM_NAME'
JOIN votes_mart.dim_hackle_event_text_attribute_v2 AS hpage
  ON hpage.text_attribute_sk = h.page_name_attribute_sk
 AND hpage.attribute_type = 'PAGE_NAME'
WHERE hek.attribute_type = 'EVENT_KEY'
  AND hek.attribute_value_raw IN (
      'view_shop', 'click_purchase', 'complete_purchase'
  );

SET @value_event_branch_rows = ROW_COUNT();
SET @value_event_loaded_cumulative =
    @value_event_loaded_cumulative + @value_event_branch_rows;
SELECT
    'hackle_events' AS loaded_branch,
    @value_event_branch_rows AS branch_row_count,
    @value_event_loaded_cumulative AS cumulative_row_count,
    2476377 AS expected_cumulative_row_count,
    IF(@value_event_loaded_cumulative=2476377,'MATCH','DIFF') AS expected_count_status;

ALTER TABLE votes_mart.mart_value_event_v2
    ADD KEY idx_value_user_time (service_user_id, event_at_raw),
    ADD KEY idx_value_type_time (event_type, event_at_raw);

