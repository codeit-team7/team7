param(
    [string]$MySqlUser = 'root',
    [string]$MySqlHost = 'localhost',
    [int]$MySqlPort = 3306,
    [ValidateSet('all', 'stage1', 'stage2', 'stage2_hackle', 'stage2_hackle_resume', 'stage2_hackle_finalize', 'stage3', 'stage3_recover', 'stage3_activity_recover', 'stage3_activity_resume_after5', 'stage3_activity_resume_after7')]
    [string]$Stage = 'all'
)

$ErrorActionPreference = 'Stop'

$mysqlExe = 'C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe'
$pythonExe = 'C:\Users\lucy5\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
$bundleDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$validator = Join-Path $bundleDir 'validate_sql_bundle.py'
$buildFileName = switch ($Stage) {
    'stage1' { '08a_build_stage_1_core_v2.sql' }
    'stage2' { '08b_build_stage_2_question_hackle_v2.sql' }
    'stage2_hackle' { '08b2_build_stage_2_hackle_only_v2.sql' }
    'stage2_hackle_resume' { '08b3_build_stage_2_hackle_resume_v2.sql' }
    'stage2_hackle_finalize' { '08b4_build_stage_2_hackle_finalize_v2.sql' }
    'stage3' { '08c_build_stage_3_value_activity_qa_v2.sql' }
    'stage3_recover' { '08d_build_stage_3_recover_v2.sql' }
    'stage3_activity_recover' { '08e_build_stage_3_activity_recover_v2.sql' }
    'stage3_activity_resume_after5' { '08f_build_stage_3_activity_resume_after5_v2.sql' }
    'stage3_activity_resume_after7' { '08g_build_stage_3_activity_resume_after7_v2.sql' }
    default  { '08_build_all_v2.sql' }
}
$buildFile = (Join-Path $bundleDir $buildFileName).Replace('\', '/')
$buildLog = Join-Path $bundleDir "last_build_v2_$Stage.log"

# 단계별 최소 여유 공간. CTAS 직후 인덱스를 추가할 때 원본/재구축본/정렬 임시파일이
# 동시에 존재할 수 있으므로 최종 테이블 예상 크기보다 보수적으로 잡는다.
$minimumFreeSpaceGB = switch ($Stage) {
    'stage1' { 30 }
    'stage2' { 25 }
    'stage2_hackle' { 25 }
    'stage2_hackle_resume' { 20 }
    'stage2_hackle_finalize' { 15 }
    'stage3' { 15 }
    'stage3_recover' { 15 }
    'stage3_activity_recover' { 15 }
    'stage3_activity_resume_after5' { 15 }
    'stage3_activity_resume_after7' { 15 }
    default  { 40 }
}

# 후속 단계는 앞 단계의 "완료된 전체 객체 묶음"이 존재할 때만 실행한다.
# 단순히 직접 참조하는 최소 테이블만 보지 않는 이유는 stage3 마지막의 25개 원천
# 커버리지 QA가 앞 단계의 지원 fact/dim/bridge까지 모두 필요로 하기 때문이다.
$stage1Objects = @(
    'dim_school_current_v2',
    'dim_group_current_v2',
    'mart_user_contact_record_v2',
    'mart_user_acquisition_profile_v2',
    'bridge_current_friend_edge_v2',
    'bridge_school_neighbor_v2',
    'bridge_contact_inviter_v2',
    'mart_friend_request_event_v2',
    'mart_friend_request_pair_summary_v2',
    'vw_friend_request_event_current_context_v2',
    'mart_user_viral_profile_v2',
    'mart_school_viral_daily_v2'
)

$stage2QuestionObjects = @(
    'dim_question_v2',
    'mart_question_piece_record_v2',
    'mart_question_set_record_v2',
    'bridge_question_set_piece_v2',
    'mart_vote_record_v2',
    'bridge_question_piece_owner_v2',
    'mart_question_candidate_exposure_v2',
    'bridge_question_owner_candidate_relation_v2',
    'vw_question_candidate_analysis_v2'
)

$stage2HacklePrefixObjects = @(
    'meta_hackle_build_v2',
    'bridge_hackle_session_property_raw_v2',
    'bridge_hackle_device_property_raw_v2',
    'dim_hackle_user_property_v2',
    'dim_hackle_event_text_attribute_v2'
)

$stage2HackleObjects = $stage2HacklePrefixObjects + @(
    'dim_hackle_device_resolved_v2',
    'dim_hackle_user_resolved_v2',
    'dim_hackle_session_resolved_v2',
    'fact_hackle_event_24d_v2',
    'bridge_hackle_event_visit_assignment_v2',
    'dim_hackle_visit_30m_v2',
    'mart_hackle_event_enriched_24d_v2',
    'vw_hackle_visit_session_24d_v2'
)

$stage2HackleThroughAssignmentObjects = $stage2HacklePrefixObjects + @(
    'dim_hackle_device_resolved_v2',
    'dim_hackle_user_resolved_v2',
    'dim_hackle_session_resolved_v2',
    'fact_hackle_event_24d_v2',
    'bridge_hackle_event_visit_assignment_v2'
)

$stage2Objects = $stage2QuestionObjects + $stage2HackleObjects

# stage3_activity_recover는 이미 완료된 가치/안전 및 06-A~C 객체에서 재개한다.
$stage3ActivityRecoveryObjects = @(
    'mart_value_event_v2',
    'mart_safety_event_v2',
    'mart_lifecycle_event_v2',
    'mart_attendance_record_v2',
    'bridge_attendance_day_v2',
    'dim_source_observation_calendar_v2'
)

$requiredObjects = switch ($Stage) {
    'stage2' { $stage1Objects }
    'stage2_hackle' { $stage1Objects + $stage2QuestionObjects }
    'stage2_hackle_resume' {
        $stage1Objects + $stage2QuestionObjects + $stage2HacklePrefixObjects
    }
    'stage2_hackle_finalize' {
        $stage1Objects + $stage2QuestionObjects + $stage2HackleThroughAssignmentObjects
    }
    'stage3' { $stage1Objects + $stage2Objects }
    'stage3_recover' { $stage1Objects + $stage2Objects }
    'stage3_activity_recover' {
        $stage1Objects + $stage2Objects + $stage3ActivityRecoveryObjects
    }
    'stage3_activity_resume_after5' {
        $stage1Objects + $stage2Objects + $stage3ActivityRecoveryObjects + @(
            '_wrk_user_activity_accum_v2'
        )
    }
    'stage3_activity_resume_after7' {
        $stage1Objects + $stage2Objects + $stage3ActivityRecoveryObjects + @(
            '_wrk_user_activity_accum_v2',
            '_wrk_user_activity_ping_day_v2',
            '_wrk_user_activity_value_day_v2'
        )
    }
    default  { @() }
}

if (-not (Test-Path -LiteralPath $mysqlExe)) {
    throw "MySQL client를 찾을 수 없습니다: $mysqlExe"
}

$systemDrive = Get-PSDrive -Name C
$freeSpaceGB = [math]::Round($systemDrive.Free / 1GB, 1)
Write-Host "현재 C드라이브 여유 공간: $freeSpaceGB GB"
Write-Host "요청 단계($Stage)의 최소 권장 여유 공간: $minimumFreeSpaceGB GB"
if ($freeSpaceGB -lt $minimumFreeSpaceGB) {
    throw "C드라이브 여유 공간이 $minimumFreeSpaceGB GB 미만이라 $Stage 구축을 시작하지 않습니다."
}

Write-Host '[1/2] SQL 묶음 오프라인 검사'
& $pythonExe $validator
if ($LASTEXITCODE -ne 0) {
    throw '오프라인 검사가 실패하여 DB 구축을 시작하지 않습니다.'
}

Write-Host ''
Write-Host "[2/2] MySQL v2 마트 구축 ($Stage)"
Write-Host '잠시 후 MySQL 비밀번호를 직접 입력하세요.'
Write-Host '압축형 star schema이지만 대용량 원천을 처리하므로 오래 걸릴 수 있습니다.'
Write-Host '화면의 단계 표시로 진행 상황을 확인할 수 있습니다.'
Write-Host '기존 7개 마트는 변경하지 않고 _v2 객체만 재생성합니다.'
Write-Host "실행 기록: $buildLog"

$executionSql = "SOURCE $buildFile;"
if ($requiredObjects.Count -gt 0) {
    # Windows PowerShell 5.1이 native exe 인수 안의 JSON 큰따옴표를 제거할 수 있다.
    # JSON_TABLE 대신 안전하게 인용한 TEMPORARY TABLE로 선행 객체 목록을 전달한다.
    $requiredObjectValues = ($requiredObjects | ForEach-Object {
        $escapedObjectName = $_.Replace("'", "''")
        "('$escapedObjectName')"
    }) -join ",`n"
    $preflightSql = @"
SET SESSION group_concat_max_len = 1048576;
USE votes_mart;
DROP TEMPORARY TABLE IF EXISTS tmp_v2_required_objects;
CREATE TEMPORARY TABLE tmp_v2_required_objects (
    object_name VARCHAR(128) NOT NULL PRIMARY KEY
);
INSERT INTO tmp_v2_required_objects (object_name) VALUES
$requiredObjectValues;
SET @missing_v2_prerequisite_count = (
    SELECT COUNT(*)
    FROM tmp_v2_required_objects AS req
    LEFT JOIN information_schema.TABLES AS t
      ON t.TABLE_SCHEMA='votes_mart'
     AND t.TABLE_NAME=req.object_name
    WHERE t.TABLE_NAME IS NULL
);
SELECT
    '$Stage' AS requested_stage,
    @missing_v2_prerequisite_count AS missing_prerequisite_count,
    GROUP_CONCAT(req.object_name ORDER BY req.object_name SEPARATOR ', ')
        AS missing_prerequisite_objects
FROM tmp_v2_required_objects AS req
LEFT JOIN information_schema.TABLES AS t
  ON t.TABLE_SCHEMA='votes_mart'
 AND t.TABLE_NAME=req.object_name
WHERE t.TABLE_NAME IS NULL;
SET @v2_prerequisite_guard_sql = IF(
    @missing_v2_prerequisite_count=0,
    'DO 0',
    'SELECT * FROM votes_mart.__V2_STAGE_PREREQUISITES_MISSING__'
);
PREPARE stmt_v2_prerequisite_guard FROM @v2_prerequisite_guard_sql;
EXECUTE stmt_v2_prerequisite_guard;
DEALLOCATE PREPARE stmt_v2_prerequisite_guard;
DROP TEMPORARY TABLE tmp_v2_required_objects;
SOURCE $buildFile;
"@
    $executionSql = $preflightSql
}

# Windows PowerShell 5.1은 native stderr를 ErrorRecord로 바꾸며, 전역 Stop 설정이면
# Tee-Object가 오류 본문을 로그에 쓰기 전에 스크립트가 끝날 수 있다.
$savedErrorActionPreference = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
try {
    & $mysqlExe `
        --host=$MySqlHost `
        --port=$MySqlPort `
        --user=$MySqlUser `
        -p `
        --default-character-set=utf8mb4 `
        --show-warnings `
        --execute=$executionSql 2>&1 | Tee-Object -FilePath $buildLog
    $mysqlExitCode = $LASTEXITCODE
}
finally {
    $ErrorActionPreference = $savedErrorActionPreference
}

if ($mysqlExitCode -ne 0) {
    throw "MySQL 구축이 중단되었습니다. 종료 코드: $mysqlExitCode. 로그: $buildLog"
}

Write-Host ''
Write-Host "요청한 구축 단계($Stage)가 끝났습니다."
if ($Stage -eq 'all' -or $Stage -eq 'stage3' -or $Stage -eq 'stage3_recover' -or $Stage -eq 'stage3_activity_recover' -or $Stage -eq 'stage3_activity_resume_after5' -or $Stage -eq 'stage3_activity_resume_after7') {
    Write-Host '아래 두 QA 테이블의 모든 행이 PASS인지 확인하세요.'
    Write-Host '- votes_mart.mart_build_source_coverage_qa_v2'
    Write-Host '- votes_mart.mart_build_integrity_qa_v2'
}
