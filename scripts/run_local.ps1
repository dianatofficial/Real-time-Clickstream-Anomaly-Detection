# PowerShell Launcher for Windows
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host " Starting Clickstream Anomaly Detection Local Environment " -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan

Write-Host "[1/3] Starting Docker services (Kafka, Zookeeper, MinIO, UI)..." -ForegroundColor Yellow
docker compose -f docker/docker-compose.yml up -d zookeeper kafka kafka-ui minio create-buckets

Write-Host "[2/3] Starting Stream Processing, Producer, and Dashboard..." -ForegroundColor Yellow
docker compose -f docker/docker-compose.yml up -d producer spark-streaming dashboard

Write-Host "================================================================" -ForegroundColor Green
Write-Host " All services are running:" -ForegroundColor Green
Write-Host "   - Kafka UI:   http://localhost:8080" -ForegroundColor White
Write-Host "   - MinIO S3:   http://localhost:9001 (minioadmin / minioadmin)" -ForegroundColor White
Write-Host "   - Dashboard:  http://localhost:8501" -ForegroundColor White
Write-Host "================================================================" -ForegroundColor Green
