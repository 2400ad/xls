@echo off
echo Git 상태 확인
git status
echo.

echo 모든 변경 사항 추가
git add -A
echo.

REM echo 커밋 메시지와 함께 커밋:
REM set /p commitMessage="커밋 메시지를 입력하세요: "
REM git commit -m "%commitMessage%"
REM git commit -m "%commitMessage%"
REM echo.

echo 현재 시간을 커밋 메시지로 사용:
for /f "tokens=2-4 delims=/ " %%a in ('date /t') do (set mydate=%%a-%%b-%%c)
for /f "tokens=1-2 delims=: " %%a in ('time /t') do (set mytime=%%a%%b)
set commitMessage=%mydate%_%mytime%
git commit -m "%commitMessage%"
echo.

echo 원격 저장소에 푸시:
git push origin main

echo 완료!
