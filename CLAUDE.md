# Auto Invest - 한국 주식 자동매매 프로그램

## 프로젝트 개요
- Python 기반 한국 주식 자동매매 시스템
- 대상: 한국투자증권 Open API (KIS API)
- 전략: 추후 결정 (모듈식으로 확장 가능하게 설계)

## 기술 스택
- Python 3.13+
- 패키지 관리: pyproject.toml + pip
- 가상환경: venv (.venv/)
- Linter/Formatter: ruff

## 코딩 컨벤션
- 언어: 코드/변수명은 영어, 주석/문서는 한국어
- 타입 힌트 사용 권장
- 모듈별 분리: config, api, strategy, core, utils
- 민감정보(API키, 비밀번호)는 .env 파일로 관리, 절대 커밋하지 않음

## 프로젝트 구조
```
auto-invest/
├── CLAUDE.md
├── pyproject.toml
├── .env.example      # 환경변수 템플릿
├── .gitignore
├── src/
│   └── auto_invest/
│       ├── __init__.py
│       ├── config.py     # 설정 관리
│       ├── api/          # 증권사 API 연동
│       ├── strategy/     # 매매 전략
│       ├── core/         # 핵심 로직 (주문, 포트폴리오)
│       └── utils/        # 유틸리티
└── tests/
```

## 명령어
- `python -m auto_invest` : 프로그램 실행
- `ruff check src/` : 린트 검사
- `ruff format src/` : 코드 포맷팅
- `pytest tests/` : 테스트 실행
