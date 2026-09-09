/* ============================================================================
   Stage 1 one-time precision repair

   Code 1265가 발생한 기존 CTAS 결과에서 수락률 열의 DECIMAL 형식을 넓힌 뒤,
   함께 보존된 분자·분모 count로 값을 다시 계산한다. 원천 행이나 상세 원장은
   다시 만들지 않는다. 02 본문이 정밀 CAST로 수정된 이후의 신규 빌드에는
   이 복구 파일이 필요하지 않다.
============================================================================ */

ALTER TABLE votes_mart.mart_user_viral_profile_v2
    MODIFY sent_eventual_acceptance_rate_including_pending DECIMAL(18,10) NULL,
    MODIFY sent_eventual_decision_acceptance_rate DECIMAL(18,10) NULL,
    MODIFY received_eventual_acceptance_rate_including_pending DECIMAL(18,10) NULL;

UPDATE votes_mart.mart_user_viral_profile_v2
SET
    sent_eventual_acceptance_rate_including_pending = CAST(
        CASE WHEN sent_request_count=0 THEN NULL
             ELSE CAST(sent_final_accepted_count AS DECIMAL(20,6))
                  / NULLIF(CAST(sent_request_count AS DECIMAL(20,6)),0)
        END AS DECIMAL(18,10)
    ),
    sent_eventual_decision_acceptance_rate = CAST(
        CASE WHEN sent_final_accepted_count+sent_final_rejected_count=0 THEN NULL
             ELSE CAST(sent_final_accepted_count AS DECIMAL(20,6))
                  / NULLIF(
                      CAST(sent_final_accepted_count+sent_final_rejected_count AS DECIMAL(20,6)),
                      0
                    )
        END AS DECIMAL(18,10)
    ),
    received_eventual_acceptance_rate_including_pending = CAST(
        CASE WHEN received_request_count=0 THEN NULL
             ELSE CAST(received_final_accepted_count AS DECIMAL(20,6))
                  / NULLIF(CAST(received_request_count AS DECIMAL(20,6)),0)
        END AS DECIMAL(18,10)
    );

ALTER TABLE votes_mart.mart_school_viral_daily_v2
    MODIFY sent_eventual_acceptance_rate_including_pending DECIMAL(18,10) NULL,
    MODIFY sent_eventual_decision_acceptance_rate DECIMAL(18,10) NULL,
    MODIFY received_eventual_acceptance_rate_including_pending DECIMAL(18,10) NULL;

UPDATE votes_mart.mart_school_viral_daily_v2
SET
    sent_eventual_acceptance_rate_including_pending = CAST(
        CASE WHEN sent_request_created_count=0 THEN NULL
             ELSE CAST(sent_created_final_accepted_count AS DECIMAL(20,6))
                  / NULLIF(CAST(sent_request_created_count AS DECIMAL(20,6)),0)
        END AS DECIMAL(18,10)
    ),
    sent_eventual_decision_acceptance_rate = CAST(
        CASE WHEN sent_created_final_accepted_count+sent_created_final_rejected_count=0 THEN NULL
             ELSE CAST(sent_created_final_accepted_count AS DECIMAL(20,6))
                  / NULLIF(
                      CAST(sent_created_final_accepted_count+sent_created_final_rejected_count AS DECIMAL(20,6)),
                      0
                    )
        END AS DECIMAL(18,10)
    ),
    received_eventual_acceptance_rate_including_pending = CAST(
        CASE WHEN received_request_created_count=0 THEN NULL
             ELSE CAST(received_created_final_accepted_count AS DECIMAL(20,6))
                  / NULLIF(CAST(received_request_created_count AS DECIMAL(20,6)),0)
        END AS DECIMAL(18,10)
    );

SELECT
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='votes_mart'
  AND TABLE_NAME IN ('mart_user_viral_profile_v2','mart_school_viral_daily_v2')
  AND COLUMN_NAME IN (
      'sent_eventual_acceptance_rate_including_pending',
      'sent_eventual_decision_acceptance_rate',
      'received_eventual_acceptance_rate_including_pending'
  )
ORDER BY TABLE_NAME, COLUMN_NAME;

SELECT
    'RATE_PRECISION_REPAIR_STATEMENTS_COMPLETED_CHECK_QA_BELOW' AS repair_status,
    (SELECT COUNT(*) FROM votes_mart.mart_user_viral_profile_v2) AS user_profile_rows,
    (SELECT COUNT(*) FROM votes_mart.mart_school_viral_daily_v2) AS school_daily_rows;

SELECT
    'user_rate_out_of_range_rows' AS test_name,
    COUNT(*) AS actual_value,
    0 AS expected_value,
    CASE WHEN COUNT(*)=0 THEN 'PASS' ELSE 'FAIL' END AS qa_status
FROM votes_mart.mart_user_viral_profile_v2
WHERE sent_eventual_acceptance_rate_including_pending NOT BETWEEN 0 AND 1
   OR sent_eventual_decision_acceptance_rate NOT BETWEEN 0 AND 1
   OR received_eventual_acceptance_rate_including_pending NOT BETWEEN 0 AND 1

UNION ALL

SELECT
    'school_daily_rate_out_of_range_rows',
    COUNT(*),
    0,
    CASE WHEN COUNT(*)=0 THEN 'PASS' ELSE 'FAIL' END
FROM votes_mart.mart_school_viral_daily_v2
WHERE sent_eventual_acceptance_rate_including_pending NOT BETWEEN 0 AND 1
   OR sent_eventual_decision_acceptance_rate NOT BETWEEN 0 AND 1
   OR received_eventual_acceptance_rate_including_pending NOT BETWEEN 0 AND 1

UNION ALL

SELECT
    'user_rate_value_mismatch_rows',
    COUNT(*),
    0,
    CASE WHEN COUNT(*)=0 THEN 'PASS' ELSE 'FAIL' END
FROM votes_mart.mart_user_viral_profile_v2
WHERE NOT (
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

UNION ALL

SELECT
    'school_daily_rate_value_mismatch_rows',
    COUNT(*),
    0,
    CASE WHEN COUNT(*)=0 THEN 'PASS' ELSE 'FAIL' END
FROM votes_mart.mart_school_viral_daily_v2
WHERE NOT (
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
      );
