# IOV Data Collector Worker

BrightData 데이터 수집 전용 워커 서버입니다. 메인 SaaS 서버와 분리되어 24시간 독립 운영됩니다.

## 아키텍처

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Main SaaS      │     │     Redis       │     │  Data Collector │
│  (Next.js)      │◀───▶│   (BullMQ)      │◀────│  (Node.js/Win)  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                                               │
        │ Heartbeat API                                 │
        │◀──────────────────────────────────────────────┘
        │                                               │
        ▼                                               ▼
┌─────────────────┐                            ┌─────────────────┐
│   PostgreSQL    │                            │   BrightData    │
│      DB         │                            │      API        │
└─────────────────┘                            └─────────────────┘
```

## 기능

- **Google Sheets 연동**: 수집할 ID 리스트를 Google Sheets에서 자동 동기화
- **BrightData 수집**: Instagram/TikTok 프로필, 해시태그, 키워드 수집
- **Queue 기반 처리**: BullMQ로 안정적인 작업 관리 및 재시도
- **자동 스케줄링**: 주기적 수집 및 DB 작업 폴링
- **상태 리포팅**: 메인 서버에 Heartbeat 전송, 관리자 페이지에서 모니터링

## Windows 설치

### 1. Node.js 설치
[Node.js 공식 사이트](https://nodejs.org/)에서 LTS 버전 다운로드 및 설치

### 2. Redis 설치 (Windows)

**방법 1: Docker Desktop (권장)**
```powershell
# Docker Desktop 설치 후
docker run -d --name redis -p 6379:6379 --restart always redis
```

**방법 2: Memurai (Windows용 Redis 대체)**
[Memurai](https://www.memurai.com/) 다운로드 및 설치 (무료 버전 사용 가능)

**방법 3: WSL2 + Redis**
```powershell
# WSL2 설치 후 Ubuntu에서
wsl
sudo apt update
sudo apt install redis-server
sudo service redis-server start
```

### 3. 프로젝트 설치

```powershell
cd data-collector
npm install
```

### 4. 환경 설정

`.env.example`을 복사하여 `.env` 파일 생성:

```powershell
copy .env.example .env
```

필수 환경 변수:
```env
# Redis (Docker 사용 시 localhost)
REDIS_HOST=localhost
REDIS_PORT=6379

# PostgreSQL (메인 SaaS DB)
DATABASE_URL=postgresql://user:password@your-server:5432/iov_saas

# BrightData
BRIGHTDATA_API_TOKEN=your_token
BRIGHTDATA_DATASET_ID_INSTAGRAM=gd_xxx
BRIGHTDATA_DATASET_ID_TIKTOK=gd_xxx

# Heartbeat (메인 SaaS 서버 URL)
SAAS_API_URL=https://your-saas-domain.com
SAAS_WORKER_SECRET=your_secret_key

# Worker 식별
WORKER_ID=home-pc-01
WORKER_NAME=집 컴퓨터
```

## 실행

### 개발 모드
```powershell
npm run dev
```

### 프로덕션 모드
```powershell
npm run build
npm run start:prod
```

### 수동 수집 (CLI)
```powershell
# Instagram 프로필 수집
npm run dev collect instagram profile @username1 @username2

# TikTok 해시태그 수집
npm run dev collect tiktok hashtag trending viral
```

## Windows 24시간 운영

### 방법 1: PM2 (권장)

```powershell
# PM2 전역 설치
npm install -g pm2
npm install -g pm2-windows-startup

# 워커 시작
cd data-collector
npm run build
pm2 start dist/index.js --name iov-collector

# Windows 시작 시 자동 실행 설정
pm2-startup install
pm2 save
```

### 방법 2: Windows 서비스 (node-windows)

```powershell
# node-windows 설치
npm install -g node-windows

# 서비스 등록 스크립트 실행
node scripts/install-service.js
```

### 방법 3: 작업 스케줄러

1. 작업 스케줄러 열기 (taskschd.msc)
2. "기본 작업 만들기" 선택
3. 트리거: "컴퓨터 시작 시"
4. 동작: 프로그램 시작
   - 프로그램: `node`
   - 인수: `dist/index.js`
   - 시작 위치: `C:\path\to\data-collector`

## Google Sheets 형식

수집할 ID 리스트 시트 형식:

| ID | Platform | Type | ProjectID |
|----|----------|------|-----------|
| @username1 | instagram | profile | 1 |
| #hashtag1 | tiktok | hashtag | 2 |
| keyword | instagram | keyword | 1 |

## 관리자 페이지에서 모니터링

워커가 실행되면 30초마다 메인 SaaS 서버에 Heartbeat를 전송합니다.
관리자 페이지 (`/admin/workers`)에서 다음을 확인할 수 있습니다:

- 워커 연결 상태 (Online/Offline)
- 마지막 Heartbeat 시간
- 처리된 작업 수
- 현재 큐 상태
- 시스템 리소스 (CPU, 메모리)

## 트러블슈팅

### Redis 연결 실패
```powershell
# Docker Redis 상태 확인
docker ps
docker logs redis

# Memurai 상태 확인
memurai-cli ping
```

### 방화벽 설정
Windows 방화벽에서 Node.js 허용 필요:
1. Windows Defender 방화벽 → 고급 설정
2. 인바운드 규칙 → 새 규칙
3. 프로그램 → Node.js 경로 지정
4. 허용

### 메인 서버 연결 실패
- `SAAS_API_URL` 확인
- `SAAS_WORKER_SECRET` 일치 여부 확인
- 네트워크/방화벽 설정 확인
# data-worker
