@echo off
color a
echo ==============================================
echo Clean Docker Build + OCI Export 
echo Eskom PE Gateway 
echo ==============================================
echo.

REM ────────────────────────────────────────────────
REM Change to the project root directory
REM ────────────────────────────────────────────────
cd /d "C:\Users\juan\Downloads\temp\dnp3_gateway_project\thingsboard-gateway"

if errorlevel 1 (
    echo ERROR: Cannot change to project directory!
    echo Path attempted: C:\Users\juan\Downloads\temp\dnp3_gateway_project\thingsboard-gateway
    pause
    exit /b 1
)

echo Current directory: %CD%
echo.

echo ========================================
echo Step 1: Stopping and removing old container
echo ========================================
docker stop eskom-tb-gateway  2>nul
docker rm   eskom-tb-gateway  2>nul
echo Done.
echo.

echo ========================================
echo Step 2: Pruning unused Docker objects
echo ========================================
docker system prune -f --volumes 2>nul
echo Done.
echo.

echo ========================================
echo Step 3: Building image from scratch
echo ========================================
docker build --no-cache -t eskom-pe-gateway:latest -f docker/Dockerfile .

if %ERRORLEVEL% NEQ 0 (
    color c
    echo.
    echo ERROR: Build failed!
    echo.
    pause
    exit /b 1
)

echo.
color d
echo =================================================
echo Step 4: Exporting image (OCI format) to .tar
echo =================================================

docker save --output eskom-pe-gateway-oci.tar eskom-pe-gateway:latest

if %ERRORLEVEL% NEQ 0 (
    color c
    echo.
    echo ERROR: Export failed!
    echo.
    pause
    exit /b 1
)

echo.
color E
echo ========================================
echo          SUCCESS!
echo ========================================
echo.
echo Image name     : eskom-pe-gateway:latest
echo Exported file  : eskom-pe-gateway-oci.tar    (OCI layout format)
echo Location       : %CD%\eskom-pe-gateway-oci.tar
echo.
echo Next steps on target Linux server:
echo   1. Copy eskom-pe-gateway-oci.tar to the server
echo   2. Load image:
echo      docker load -i eskom-pe-gateway-oci.tar
echo   3. docker stop  eskom-tb-gateway
echo   4. docker rm    eskom-tb-gateway
echo   5. docker-compose up -d
echo   6. docker logs -f eskom-tb-gateway
echo.
pause