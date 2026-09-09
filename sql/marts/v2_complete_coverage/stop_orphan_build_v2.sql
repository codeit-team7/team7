/*
  앞서 연결이 끊어진 구형 bridge_current_friend_edge_v2 CTAS만 중단한다.
  현재 이 스크립트를 실행하는 연결과 Sleep 연결은 대상에서 제외한다.
*/
SET @orphan_build_process_id = (
    SELECT ID
    FROM information_schema.PROCESSLIST
    WHERE ID <> CONNECTION_ID()
      AND COMMAND <> 'Sleep'
      AND INFO LIKE '%bridge_current_friend_edge_v2%'
    ORDER BY TIME DESC
    LIMIT 1
);

SELECT
    @orphan_build_process_id AS target_process_id,
    CASE WHEN @orphan_build_process_id IS NULL
         THEN 'NO_MATCHING_ORPHAN_QUERY'
         ELSE 'MATCHED_ORPHAN_QUERY_WILL_BE_KILLED' END AS cleanup_status;

SET @kill_orphan_sql = CASE
    WHEN @orphan_build_process_id IS NULL THEN 'DO 0'
    ELSE CONCAT('KILL CONNECTION ', @orphan_build_process_id)
END;
PREPARE stmt_kill_orphan FROM @kill_orphan_sql;
EXECUTE stmt_kill_orphan;
DEALLOCATE PREPARE stmt_kill_orphan;

SELECT 'ORPHAN_BUILD_CLEANUP_REQUEST_FINISHED' AS cleanup_result;
