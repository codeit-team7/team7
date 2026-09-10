$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..\..\..")).Path
$pythonExe = Join-Path $repoRoot ".venv\Scripts\python.exe"
$mysqlCommand = Get-Command mysql -ErrorAction SilentlyContinue
$mysqlExe = if ($mysqlCommand) { $mysqlCommand.Source } else { "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" }
$exportScript = Join-Path $repoRoot "analysis\preprocessing\hackle_mixed\export_hackle_support.py"
$outputDir = Join-Path $repoRoot "data\processed\hackle_mixed\support"

if (-not (Test-Path -LiteralPath $pythonExe)) {
    $pythonCommand = Get-Command python -ErrorAction Stop
    $pythonExe = $pythonCommand.Source
}
if (-not (Test-Path -LiteralPath $mysqlExe)) { throw "MySQL 실행 파일을 찾지 못했습니다: $mysqlExe" }

$securePassword = Read-Host "MySQL 비밀번호를 입력하세요 (화면에 표시되지 않습니다)" -AsSecureString
$passwordPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassword)
try {
    $plainPassword = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($passwordPointer)
    $env:MYSQL_PWD = $plainPassword
    & $pythonExe $exportScript --mysql-exe $mysqlExe --output-dir $outputDir
    if ($LASTEXITCODE -ne 0) { throw "Hackle 지원 테이블 추출이 실패했습니다." }
}
finally {
    Remove-Item Env:MYSQL_PWD -ErrorAction SilentlyContinue
    $plainPassword = $null
    $securePassword.Dispose()
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($passwordPointer)
}

Write-Host "Hackle 지원 테이블 추출 완료"
