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
