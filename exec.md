# 실행 가이드 (Google Cloud VM)

## 1. 시스템 패키지 설치 (Ubuntu/Debian)

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv
```

> Python 3.12 미만인 경우 pyenv로 설치:

## 2. pyenv로 Python 3.13 설치 (Python 3.12 미만인 경우)

```bash
# 빌드 의존성 설치
sudo apt install -y build-essential libssl-dev zlib1g-dev \
  libbz2-dev libreadline-dev libsqlite3-dev curl libncursesw5-dev \
  xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev

# pyenv 설치
curl https://pyenv.run | bash

# 셸 설정 추가
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc
source ~/.bashrc

# Python 3.13 설치 및 기본 설정
pyenv install 3.13
pyenv global 3.13

# 확인
python --version
```

## 3. 프로젝트 설정

```bash
cd auto-invest

# 가상환경 생성 및 활성화
python -m venv .venv
source .venv/bin/activate

# 패키지 설치
pip install -e .
```

## 4. 환경변수 설정

```bash
cp .env.example .env
nano .env  # KIS API 키, 시크릿, 계좌번호 등 입력
```

## 5. 실행

```bash
python -m auto_invest
```

## 참고사항

- 웹 대시보드 기본 포트: **5001**
- GCP 방화벽에서 5001 포트를 열어야 외부에서 접속 가능
