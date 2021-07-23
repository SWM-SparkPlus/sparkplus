# Spark Plugin

Apache Spark Plugin to analyze Korea's address system

## 프로젝트 구조
- spark-plus: 패키지 폴더
- setup.py: 패키지의 설정, 서문
- .pypirc: 패키지 업로드를 위한 설정
- 추가: MANIFEST.in 등의 파일

## 패키지 빌드 및 업로드
1. `setuptools`, `wheel` 설치
   - `pip install setuptools wheel`
   - `python -m pip install --user --upgrade setuptools wheel`
  
2. 프로젝트 폴더에서 패키지 빌드
   - `python setup.py sdist bdist_wheel`
   - dist 폴더에 `.tar.gz`, `.whl` 파일 생성되었는지 확인

3. PyPi에 패키지 업로드
   - `pip install twine`
   - `python -m twine upload dist/*`
      - PyPi username과 password 입력