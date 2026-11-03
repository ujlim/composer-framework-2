# SSM → Cloud Build → Composer 배포 파이프라인 구현 가이드

- 작성/검증: 2026-11-03
- 기반: `feature/26.1102.selective-dag-parsing`, `f79492692984748e51462597d885f52c5c9c9423`
- 이 변경은 별도 브랜치의 가이드·스크립트입니다. GCP 리소스 생성, IAM 변경, 트리거 활성화, 실제 DAG 배포는 수행하지 않았습니다.
- 여기서 SSM은 **Google Cloud Secure Source Manager**입니다.

## 1. 브랜치와 배포 대상

| 이벤트 | 목적 | 자동 실행 대상 |
|---|---|---|
| 개발자가 `dev`에 push | 개발 중 변경 확인 | **개발 Composer DAG 버킷** |
| 배포자가 `main`에서 버전 관리 | 릴리스 이력·검토·버전 확정 | 운영 배포 없음 |
| 검토된 `main` 버전을 `prod`로 merge | 승인한 버전 운영 반영 | **운영 Composer DAG 버킷** |

SSM의 `push` 이벤트로 `^dev$`, `^prod$`를 각각 구독합니다. merge도 대상 브랜치의 push 이벤트입니다. 트리거는 'main에서 온 merge'와 직접 push를 구분하지 않으므로 **prod 직접 push를 막는 브랜치 보호와 승인된 main→prod 절차**가 필요합니다. 이름이 prod라는 이유만으로 승인된 릴리스가 되지는 않습니다. dev 계정에는 운영 버킷/운영 제어 경로 권한을 주지 않습니다.

`triggers.example.yaml`은 두 트리거를 모두 담지만 `disabled: true`, `_EXECUTE: 'false'`입니다. 운영자가 프로젝트·계정·버킷·이미지를 확정하고 개발 환경 리허설을 마친 뒤 각 트리거를 활성화해야 실제 자동 전개가 시작됩니다. `main` 자동 배포 트리거는 없습니다.

**버킷 전개는 자동으로 끝까지 처리합니다.** 활성화 후 dev push 또는 prod merge는 전체 검증→백업→버킷 갱신→전체 해시 확인→배포 state 확정→lock 해제까지 자동 실행합니다. 다음 push마다 수동 accept를 요구하지 않습니다. 동시 빌드는 최대 900초 동안 환경 lock을 기다리고, 대기 중에도 브랜치 최신 SHA를 확인합니다. 자기 커밋보다 브랜치가 진행됐으면 멈추고 최신 커밋 빌드가 담당합니다. 이전 배포가 부분 실패해 lock을 남겼다면 자동 탈취하지 않고 복구가 필요하다고 실패합니다.

자동 성공의 의미는 **검증한 전체 후보가 버킷에 정확히 게시됨**입니다. Composer scheduler/worker 정상 동작까지 확인했다는 뜻은 아닙니다. 정상 관찰·실제 smoke test는 별도이며, 실패 후에는 새 검증 릴리스로 롤백합니다. 이 구분을 조직의 운영 알림/모니터링에 반영하세요. 필요할 때만 수동 `apply` 모드로 게시 후 확정을 보류할 수 있습니다.

## 2. 파일 구성

- `.cloudbuild/triggers.example.yaml`: dev/prod SSM 트리거 예시. 기본 브랜치에서 실제 `.cloudbuild/triggers.yaml`로 설치합니다.
- `.cloudbuild/validate.yaml`: 운영 쓰기 권한 없는 사전 검증용 빌드.
- `.cloudbuild/deploy.yaml`: 환경별 사전 inventory → 전체 후보 검증 → 최신 브랜치 확인/직렬화 → 자동 게시·state 확정.
- `deployment/sources.example.json`: 전체 후보를 만드는 소스 저장소·고정 커밋 목록.
- `tools/deployment/candidate.py`: Git 객체에서 후보 조립, 해시 검증, 전체 이름/구문 검사.
- `tools/deployment/full_parse.py`: 실제 Airflow/provider에서 전체 DAG와 task graph 구성 검증.
- `tools/deployment/publish.py`: 읽기 전용 inventory/plan, 명시적인 init/apply/auto-apply/accept/recover.
- `tools/deployment/inspect_composer.sh`: Composer 환경·import 오류·DAG 목록 조회. task를 실행하지 않습니다.
- `tests/test_deployment_pipeline.py`: 실패 주입을 포함한 오프라인 테스트.

## 3. 먼저 확정할 환경 정보

dev/prod 각각 다음을 작성하고 실제 값을 검증합니다.

1. GCP project, Composer environment, region, 실제 `config.dagGcsPrefix`.
2. 전체 DAG 배포에 참여하는 저장소 목록과 소유 경로. 한 저장소만 환경 전체라는 가정을 하지 않습니다.
3. Composer의 정확한 Python/Airflow/provider 버전과 검증 이미지 digest.
4. 전용 개발/운영 배포 계정, 별도 사전 검증 계정, 저장소 읽기 권한.
5. Composer와 **다른 버킷**의 배포 제어 경로. dev/prod는 별도 경로와 권한을 사용합니다.
6. 브랜치 보호, 배포/복구 담당자, 동기화 확인 기준, 변경 가능한 운영 시간.

```sh
# 읽기 전용: 추측한 버킷명을 쓰지 말고 이 결과를 사용합니다.
gcloud composer environments describe ENVIRONMENT \
  --project PROJECT --location LOCATION --format='value(config.dagGcsPrefix)'
```

스크립트는 해당 환경의 **`gs://BUCKET/dags` 전체**를 하나의 배포 단위로 관리합니다. 버킷 루트, `plugins/`, `logs/`, `data/`는 건드리지 않습니다. 기존에 다른 팀이 관리하는 DAG가 같은 `dags/` 아래 있다면 그 저장소도 통합 후보에 포함시켜 공동 배포 체계를 먼저 구성하세요. 별도 Python DAG entry point, `.airflowignore`, 심볼릭 링크, submodule을 포함하는 배포는 이 구현의 지원 범위 밖이며 조립 시 차단됩니다. 그런 환경을 이 스크립트에 억지로 맞추거나 누락시키지 말고 설계를 확장·검증해야 합니다.

## 4. 통합 후보: 여러 저장소도 반드시 한 번에 검사

검증 대상은 Git diff나 이번 저장소만이 아니라 **배포 후 존재해야 하는 전체 dags 트리**입니다. 변경하지 않는 저장소도 마지막 승인 SHA로 포함하고, 삭제는 새 전체 후보에서 빠진 경로로 표현합니다. 기존 운영 버킷에서 일부 파일만 덮어씌워 후보를 만들지 않습니다.

예시(별도 `deployment/sources.prod.json`에 저장하고 `_SOURCES_LOCK`으로 선택):

```json
{
  "sources": [
    {"name": "framework", "repository": "..", "commit": "$COMMIT_SHA", "dags": "dags"},
    {"name": "finance-pipelines", "repository": "../../finance-pipelines", "commit": "1111111111111111111111111111111111111111", "dags": "dags"}
  ]
}
```

`repository`는 sources JSON 기준 상대 경로의 **로컬 Git checkout**입니다. 예시 SHA는 실제로 존재하는 승인 SHA로 바꿔야 합니다. 다른 저장소는 앞 단계에서 SSM 읽기 계정으로 해당 SHA를 fetch해 준비합니다. 이 도구는 외부 저장소를 추측하거나 자동 발견하지 않습니다. 그 목록이 환경 전체인지 검토하는 것은 배포 설계의 필수 조건입니다.

`candidate.py`는 `git ls-tree`와 `git cat-file`로 정확한 commit 객체의 파일을 읽습니다. dirty worktree, untracked 파일, 움직이는 branch/tag 이름은 후보에 섞이지 않습니다. 두 저장소가 같은 파일 경로를 제공하면 내용이 같아도 실패합니다. framework와 main.py는 한 저장소만 소유하고, 업무 저장소는 충돌하지 않는 `pipelines/roots/...`, `pipelines/vines/...`를 제공합니다.

**Cloud Build 소스 준비 전제:** 실행 위치에 정확한 Git 객체와 `origin` remote가 있어야 합니다. SSM 소스 전달 방식/빌드 설정에 따라 `.git`이 없다면 읽기 계정으로 명시적 clone/fetch 단계를 추가하고 `$COMMIT_SHA`가 실제 checkout SHA인지 확인하세요. 임의의 최신 branch checkout으로 대체하지 않습니다. 템플릿은 Git 객체가 없거나 `git ls-remote origin refs/heads/dev|prod`로 최신 커밋을 확인하지 못하면 실패합니다. Git 인증은 SSM의 공식 계정 흐름으로 설정하고 토큰/키를 소스에 저장하지 않습니다.

```sh
# 로컬에서 cloud access 없이 후보 조립/정적 검증
COMMIT=$(git rev-parse HEAD)
python tools/deployment/candidate.py assemble \
  --sources deployment/sources.example.json --source-commit "$COMMIT" --output /tmp/composer-candidate
python tools/deployment/candidate.py validate --candidate /tmp/composer-candidate
```

후보는 `dags/`와 `manifest.json`으로 구성됩니다. manifest에 전체 파일 SHA-256, 소스 commit 목록, release digest를 기록합니다. 기존 출력 디렉터리를 재사용하지 않습니다. 검증과 배포 사이 또는 import 중 파일이 바뀌면 해시 검사에서 실패합니다. 이 manifest는 **배포 아티팩트**일 뿐 runtime 선택 파싱 캐시로 사용하지 않습니다.

## 5. 배포 전 실패 조건

### 정적 전체 검사

기존 `tools/validate_pipeline_names.py`를 통합 후보에 적용합니다. DAG ID 중복, 폴더명 중복, ID/폴더 alias 충돌, 잘못된 YAML, 중복 YAML key, 섹션 혼재, 허용하지 않는 ID, 발견 규칙 밖의 config 경로를 차단합니다. `1`과 `true`처럼 PyYAML에서 같은 mapping key로 구성되는 중복도 엄격 검사에서 실패합니다. 모든 Python 소스도 compile합니다.

**현 저장소의 알려진 차단 요인:** 기반 커밋의 35개 설정에는 중복 DAG ID는 없지만 Root/Vine 양쪽의 폴더명 `sample_business_daily`, `sample_business_monthly`, `sample_business_yearly`, `ujlim_sftp_runner_sample_daily`가 중복되어 검사 exit 1입니다. alias 충돌도 함께 보고되고, ID/폴더 불일치 warning은 8건입니다. 이는 runtime DAG ID 중복과는 별개의 운영 이름 규칙입니다. 이 브랜치는 기존 폴더를 임의 변경하지 않습니다. 활성화 전에 이름 규칙에 맞는 정리와 SQL/schema 상대 경로 영향 검토가 필요하며, 오류를 무시해 자동 배포를 켜지 마세요.

### 실제 Airflow 전체 파싱

검증 이미지는 운영 Composer와 Python/Airflow/provider 패키지가 일치하고 `git`, `PyYAML`이 설치되어 있어야 합니다. `full_parse.py`는 별도 프로세스에서 프로젝트의 parsing-context helper만 full mode로 고정하고 다음을 모두 검사합니다.

- strict catalog 개수 = runtime 발견 개수 = 생성 대상 개수 = 등록 DAG 개수.
- `target_dag_id is None`, `registration_errors == []`.
- 기대 DAG ID 집합과 실제 등록 ID 집합 일치.
- 실제 Airflow `DagBag` import 오류 없음 및 동일한 DAG 집합.
- task/dependency graph snapshot 기록 및 최종 후보 해시 재확인.

이 때문에 정상 DAG 일부만 살아남은 runtime 격리를 배포 성공으로 오해하지 않습니다. Root의 `vines[].vine_id` 공백처럼 factory 생성에 실패하는 문제도 등록 오류/누락 검사로 차단합니다. 전체 task/operator 구성은 실제 factory 경로를 사용하지만 SQL 실행, 외부 시스템 접속, task 성공까지 검증하는 것은 아닙니다.

```sh
PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s tests
PYTHONDONTWRITEBYTECODE=1 python tools/deployment/full_parse.py \
  --candidate /tmp/composer-candidate \
  --image 'REGISTRY/composer-validation@sha256:ACTUAL_64_HEX_DIGEST'
```

성공 시 `validation.json`에 release/image/전체 DAG ID/task graph를 저장합니다. 실패한 재검증은 이전 성공 보고서를 지웁니다. `apply`는 후보 해시와 이 보고서의 release가 다르거나 이미지가 digest로 고정되지 않았으면 거절합니다. 보고서는 서명된 보안 증명이 아니므로 trusted pipeline에서만 생성·소비해야 합니다.

## 6. SSM/Cloud Build 설치와 권한

SSM 연결은 공식 [Cloud Build 연결 문서](https://docs.cloud.google.com/secure-source-manager/docs/connect-cloud-build)를 따라 구성합니다. 저장소 읽기 권한, 저장소에 연결된 계정과 빌드 실행 계정의 관계를 구분하세요. 이 저장소는 IAM을 자동 생성하지 않습니다.

SSM은 기본 브랜치의 `.cloudbuild/triggers.yaml`을 읽습니다. 이 저장소의 예시를 실제 값으로 작성한 뒤 기본 브랜치(main인지 확인)에 설치하세요. `includedGitRefs`/`includedFiles`는 SSM의 RE2 표현식이며 Cloud Build의 파일 glob과 혼동하지 않습니다. 전체 후보 검사 누락을 막기 위해 예시에 changed-file 필터는 넣지 않았습니다. [공식 트리거 스키마](https://docs.cloud.google.com/secure-source-manager/docs/triggers-file-schema)를 참고하세요.

- dev 배포 계정: 개발 DAG 객체만 list/get/create/delete, 개발 제어 state/lock만 갱신, 개발 archive create/get.
- prod 배포 계정: 동일한 종류의 권한을 운영 대상으로만 부여. 개발자/PR 검증 계정과 분리.
- 사전 검증 계정: SSM 읽기, 필요한 이미지 읽기, 로그 쓰기. 운영 DAG 쓰기·운영 데이터 접근 없음.
- 제어 버킷 `operations/`는 만들어진 archive의 overwrite/delete를 금지하는 별도 권한/보존 정책을 권장합니다. state/lock은 갱신·삭제가 가능해야 하므로 같은 retention 정책으로 잠그지 않습니다.
- Composer 환경 조회/CLI 확인 계정은 필요한 `composer.environments.get`와 `composer.environments.executeAirflowCommand` 등을 별도 검토합니다. publisher 자체는 Airflow 명령 권한을 필요로 하지 않습니다.
- Cloud Logging에 쓰면 빌드 계정의 log writer 권한, Artifact Registry 이미지 읽기, private pool/네트워크 경로를 확인합니다. 기존 버킷/객체와 인증 설정을 바꾸는 작업은 별도 승인 대상입니다.

검증 이미지는 실제 Composer와 맞춘 사전 제작 이미지, 도구 이미지는 `git`, Python, `PyYAML`, `google-cloud-storage`와 필요한 SSM Git 인증 도구를 포함한 사전 제작 이미지입니다. 둘 다 digest로 고정하고 이미지 제작·dependency lock/취약점 검토를 조직 빌드 절차에 포함하세요. 이 작업 환경에는 실제 Airflow/GCS 라이브러리/Composer 이미지가 없어 해당 이미지를 빌드·배포 검증하지 않았습니다.

**보안 경계:** Cloud Build의 한 빌드 안에서 step 이름만 나눠도 서비스 계정은 분리되지 않습니다. `deploy.yaml`의 검증 코드도 그 빌드 계정 권한으로 실행됩니다. prod로 들어가는 DAG/Python/빌드 스크립트 전체를 신뢰 가능한 보호 브랜치 리뷰 대상으로 삼고, 임의 PR 코드에 prod 계정을 주지 마세요. 더 강한 격리가 필요하면 쓰기 권한 없는 별도 검증 빌드와 trusted 배포 저장소/고정 runner의 승인된 아티팩트 소비 구조로 확장하세요. dev 역시 개발 버킷 범위 밖 권한을 주면 안 됩니다.

## 7. 최초 도입: 기존 버킷 상태 등록

`init`은 live DAG를 수정하지 않습니다. 현재 전체 파일 목록과 SHA-256을 승인 기준으로 등록할 뿐, 기존 DAG가 정상이라는 증명은 아닙니다. 먼저 환경 전체 소유 범위와 기존 정상 동작을 확인하고 다른 쓰기 경로를 중단하세요. 통합 후보에서 누락된 기존 파일은 다음 plan에서 삭제 대상이 되므로 반드시 검토해야 합니다.

```sh
TARGET='gs://ACTUAL_COMPOSER_BUCKET/dags'
CONTROL='gs://SEPARATE_CONTROL_BUCKET/composer-prod' # dev는 별도 경로
python tools/deployment/publish.py inventory --target "$TARGET" --control "$CONTROL" > inventory.json
# inventory.json의 파일 전체와 inventory_sha256을 사람이 검토합니다.
python tools/deployment/publish.py init --target "$TARGET" --control "$CONTROL" \
  --expected-inventory REVIEWED_SHA256 --execute --confirm-prefix "$TARGET"
```

기존 state가 있으면 재초기화하지 않습니다. 버킷에 `dags/` 디렉터리 marker나 비정상 상대 경로 객체가 있으면 fail-closed합니다. 이런 객체의 소유/필요 여부를 별도로 검토해야 하며 임의 무시하거나 삭제하지 않습니다. 초기화 이후 예상 밖 파일 추가/변경/삭제는 plan/apply를 차단합니다.

## 8. 배포 실행 및 안전장치

빌드는 검증 전 inventory를 기록합니다. 검증을 통과하면 SSM 해당 dev/prod 브랜치의 최신 SHA와 `$COMMIT_SHA`가 같은지 확인합니다. `auto-apply`는 앞선 배포 lock이 풀릴 때까지 제한 시간 동안 기다린 뒤 **그 시점의 현재 state를 다시 읽어** 전체 후보를 plan/apply합니다. lock을 획득한 상태에서도 원격 브랜치 최신 SHA를 다시 확인하므로 이전 빌드가 나중에 완료된 신규 릴리스를 덮어쓰지 않습니다. 브랜치가 진행됐거나 조회에 실패하면 오래된 빌드를 배포하지 않습니다. 이 확인은 마지막 조회 시점의 보장이고, Git merge와 GCS 갱신을 묶는 분산 transaction은 아닙니다.

`apply`의 기본 게시 순서(자동 트리거는 이어서 state 확정까지 수행):

1. 후보/검증 결과 해시 확인.
2. 제어 버킷에 `if_generation_match=0` lock 생성. 동시 배포는 대기/충돌 처리.
3. **lock 획득 후** 현재 state 및 전체 live dags 목록/내용을 비교합니다. 다른 배포의 중간 상태를 drift로 오해하지 않습니다. 기준 state가 먼저 진행됐으면 live 변경 전 자신의 lock만 해제하고 최신 기준으로 다시 검사합니다. 진짜 불명 파일/수동 변경은 차단합니다. 자동 모드는 **lock 안에서도 원격 브랜치 SHA를 재확인**하여 오래된 빌드가 먼저 완료된 최신 릴리스를 덮어쓰지 않게 합니다.
4. 기존 객체를 **지정 generation으로 읽고**, 이전/신규 원본 바이트·해시·generation·custom metadata 및 content 속성·target·release를 immutable operation archive에 저장하고 재독출 확인.
5. 백업 직후 live generation들이 바뀌지 않았는지 재확인.
6. 변경/신규 파일을 generation 조건부 업로드. `main.py`는 변경 업로드 중 마지막.
7. 이전 accepted 목록에 있었지만 새 전체 후보에는 없는 경로만 generation 조건부 삭제.
8. 전체 목록/해시가 후보와 동일한지 확인. 수동 `apply`는 `awaiting-finalization` 상태로 lock/이전 state를 유지합니다.
9. **자동 `auto-apply`는 바로 state를 generation CAS로 확정하고 자신의 lock을 해제**하여 `published-storage-verified`, `composer_health: not-verified`를 반환합니다. `state.release`는 다음 배포 비교 기준이지 운영 건강 판정이 아닙니다.

절대로 `gsutil rsync -d` 또는 버킷 루트 재귀 삭제를 하지 않습니다. archive는 Composer 바깥 버킷에 저장하므로 백업 Python이 DAG로 탐색되지 않습니다. 원본 객체는 raw download로 받아 전송 중 gzip 해제 등으로 바이트가 바뀌지 않게 합니다. 복구 시 custom metadata와 content type/encoding/cache control/disposition/language를 복원하지만 객체 ACL, 개별 hold, custom time, encryption key 설정까지 재현하는 도구는 아닙니다. 그런 객체 정책이 있다면 사전에 설계를 확장하세요. archive는 전체 전후 내용을 메모리에 담아 저장하므로 매우 큰 DAG 트리라면 크기/비용을 측정하고 streaming archive 설계로 확장하세요.

```sh
# 읽기 전용 계획: inventory.json의 state.release를 사용
python tools/deployment/publish.py plan --candidate /tmp/composer-candidate \
  --target "$TARGET" --control "$CONTROL" --expected-release ACCEPTED_RELEASE
# 수동 실행 예시 (게시 후 별도 확정/관찰을 원하는 경우)
python tools/deployment/publish.py apply --candidate /tmp/composer-candidate \
  --target "$TARGET" --control "$CONTROL" --expected-release ACCEPTED_RELEASE \
  --execute --confirm-prefix "$TARGET"
```

`OPERATION=...` 값을 빌드 로그에서 보관합니다. timeout/통신 오류를 '실행 안 됨'으로 가정하지 않습니다. 같은 명령을 무조건 재실행하거나 lock을 지우지 마세요. [GCS generation precondition](https://docs.cloud.google.com/storage/docs/request-preconditions)은 객체 단위 동시 변경을 막지만 디렉터리 transaction을 제공하지 않습니다.

## 9. Composer 관찰과 수동 모드 확정

**main.py 마지막 업로드도 원자적 전환이 아닙니다.** Composer는 파일을 비동기로 동기화하며 scheduler와 worker도 서로 다른 시점에 반영됩니다. 기존 main이 신규 framework/일부 YAML을 보는 중간 상태가 가능합니다. 호환되지 않는 framework/schema 변경은 별도 유지보수 시간, 영향 DAG pause/진행 중 작업 확인 등 승인된 운영 절차가 필요합니다. 이 스크립트는 DAG를 pause/unpause하지 않습니다.

```sh
bash tools/deployment/inspect_composer.sh PROJECT LOCATION ENVIRONMENT /tmp/composer-evidence
```

확인 항목:

- 실제 `dagGcsPrefix`와 타깃 일치, 배포 이후 scheduler full parse 확인.
- import 오류 없음. **오류 목록 CLI exit 0만으로 정상이라고 보지 않습니다.**
- `validation.json`의 DAG ID·task/dependency graph와 실제 등록 결과 일치.
- worker 동기화 확인, 대표 Root→Vine/센서/업무일자 경로의 개발 smoke test.
- 삭제 DAG는 기존 serialized metadata/진행 중 task가 남을 수 있으므로 별도 확인. 파일 삭제가 task 중단/DB 삭제를 뜻하지 않습니다.
- 운영 실제 task trigger/데이터 변경은 배포 스크립트에 포함하지 않으며 필요한 승인 후 수행.

고정 sleep만으로 동기화 완료라고 판단하지 마세요. [Composer DAG 갱신 문서](https://docs.cloud.google.com/composer/docs/composer-3/manage-dags)도 scheduler 표시와 worker 반영을 구분합니다.

```sh
# 수동 apply로 lock을 남긴 경우에만 실행. 자동 트리거는 이미 확정합니다.
# 이 명령 자체는 건강 판정기가 아닙니다.
python tools/deployment/publish.py accept --operation OPERATION_ID \
  --target "$TARGET" --control "$CONTROL" --execute --confirm-prefix "$TARGET"
```

자동 게시 마지막 단계와 수동 accept는 모두 live 해시를 다시 확인하고 accepted state를 generation CAS로 바꾼 뒤 자신의 lock만 해제합니다. state 쓰기는 성공했지만 응답/lock 해제가 실패한 경우 같은 operation의 accept로 마무리할 수 있습니다. 이미 lock이 없는 정상 종료 상태라면 inventory의 state를 확인하고 다시 쓰지 않습니다.

## 10. 부분 실패 복구와 정상 릴리스 롤백

### A. state 확정 이전의 부분 배포 실패

원래 Cloud Build/프로세스가 완전히 종료되어 더는 쓸 수 없는지 먼저 확인합니다. 불확실하면 복구를 시작하지 않습니다. 자동 lock 만료/탈취는 없습니다.

```sh
python tools/deployment/publish.py recover --operation OPERATION_ID \
  --target "$TARGET" --control "$CONTROL" --writer-stopped \
  --execute --confirm-prefix "$TARGET"
# Composer 동기화와 이전 정상 DAG 상태를 다시 확인한 뒤
python tools/deployment/publish.py release-recovered --operation OPERATION_ID \
  --target "$TARGET" --control "$CONTROL" --writer-stopped \
  --execute --confirm-prefix "$TARGET"
```

복구는 모든 경로를 먼저 검사합니다. 원래 generation 그대로이거나 해당 operation이 기록한 내용/metadata인 경우만 되돌립니다. 알려지지 않은 동시 변경이 하나라도 있으면 쓰기 전에 멈춥니다. 기존 파일을 복원하고 신규 전용 파일만 제거하며, 각 변경에도 현재 generation 조건을 사용합니다. 업로드 성공 후 응답만 유실됐거나 복구가 다시 중단되어도 operation 표식/내용을 검사해 재개합니다. baseline state는 복구 동안 바꾸지 않습니다. 단, lock을 무시하는 다른 writer는 시스템 안전성을 깨뜨릴 수 있으므로 IAM상 단일 배포 경로가 필수입니다.

lock 생성 뒤 archive 저장 이전에 실패했다면 live를 건드리지 않습니다. 해당 operation archive가 없고 live가 accepted state와 정확히 같으며 원래 writer 종료를 확인한 경우에만 `release-unstarted`를 쓸 수 있습니다. archive가 있으면 `recover`/`accept` 경로를 사용하세요. `init` 중단은 live 쓰기가 없지만 별도 init lock/state를 확인해야 하며, 운영자가 종료 확인·상태 확인 후 generation 조건으로 lock을 정리하는 절차가 필요합니다.

### B. state 확정/자동 게시 완료 후 이전 버전으로 롤백

부분 실패용 recover를 재사용하지 않습니다. 이전 릴리스의 모든 저장소 SHA로 새 후보를 조립하고 **현재 Composer 이미지에서 전체 검증**한 뒤 현재 accepted release를 기준으로 새 plan/apply를 수행합니다. 이전 코드 파일 일부만 개별 복사하지 않습니다. 자동 트리거 롤백은 main에서 검토된 revert/복원 커밋을 버전 관리하고 prod로 merge하여 처리합니다. 움직이는 브랜치를 과거 SHA로 강제 reset하지 않습니다.

롤백도 비동기 동기화와 mixed-version 구간이 있으며 이미 실행된 SQL/Dataflow/SFTP 등 외부 side effect는 되돌리지 않습니다. 데이터 복구/재실행 여부는 별도 운영 판단입니다.

## 11. 검증 결과와 남은 도입 작업

이 브랜치에서 실제 수행한 검증:

- 신규 배포 테스트 36개, 전체 오프라인 회귀 테스트 291개 통과.
- 다중 저장소 통합/파일 소유 충돌, dirty worktree 제외, 고정 SHA 요구, 경로/심볼릭 링크 차단.
- YAML 동치 key 중복, 대상 한정 파싱/부분 factory 실패를 성공으로 처리하지 않는 검사.
- drift/동시 배포/잘못된 대상, 백업 실패 시 live 변경 0건.
- 각 upload/delete 전 실패 및 성공 후 응답 유실, 복구 재중단, archive 손상, 외부 변경 차단.
- accept state 쓰기 성공 후 응답 유실 및 재시도.
- 자동 게시가 lock을 해제해 다음 push가 진행됨, 동시 배포 대기 후 현재 state 갱신, 오래된 커밋 차단, 실패 lock 비탈취.
- 실제 기반 저장소를 후보로 조립했고 이름 중복 때문에 정적 검증이 **의도대로 실패(exit 1)**함을 확인.

미수행: 실제 Composer 이미지의 Airflow 전체 파싱, GCS API 통합, SSM 트리거 동작, IAM/네트워크/버킷 정책, 개발/운영 smoke test. 오프라인 fake storage는 GCS emulator나 실제 클라우드 검증을 대체하지 않습니다. 자동 전개 활성화 전 개발 버킷에서 위 실패/복구 시나리오를 실제 API로 재현하고 검증 이미지를 고정해야 합니다. 프로젝트·환경·버킷·서비스 계정·이미지 digest는 예시 placeholder이며 이 커밋만으로 운영 배포가 활성화되지 않습니다.
