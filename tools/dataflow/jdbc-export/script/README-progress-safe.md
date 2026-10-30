# B 전용 안전 이미지 / Flex Template 빌드

이 도구는 `feature/26.1030.dataflow-progress-logs`의 B launcher를 **별도 이미지 이름**으로 빌드합니다.
기존 `build-image.sh`, `build-template.sh`는 호환성 때문에 변경하지 않았지만 기본값이
`jdbc-export:v1` / `templates/jdbc-export/template.json`이므로 **B 배포에 사용하지 마십시오**.
현재 변경은 코드 제공뿐이며 실제 Cloud Build, GCS 업로드, Dataflow 배포를 수행하지 않았습니다.

## 출력 경로와 실행

저장소 루트에서 현재 브랜치/전체 소스를 확인한 후 실행합니다. 이 스크립트는 자신의 위치에 있는
`../docker`를 빌드하므로 다른 체크아웃에 스크립트만 복사하지 마십시오.

```bash
# 기본은 완전한 offline dry-run: 인증/API 조회/빌드/업로드 없음
python tools/dataflow/jdbc-export/script/build-progress-safe.py --version 20261030-v1

# 운영자가 경로와 사전조건을 확인한 뒤 명시적으로 실행할 때만 유료 빌드/신규 업로드
python tools/dataflow/jdbc-export/script/build-progress-safe.py --version 20261030-v1 \
  --gcs-source-staging-dir gs://approved-seoul-source-bucket/build-source \
  --gcs-log-dir gs://approved-seoul-logs-bucket/build-logs --execute
```

기본 프로젝트 `gcp-prod-edp-sa-udw`, 지역 `asia-northeast3`:
- 새 이미지: `asia-northeast3-docker.pkg.dev/gcp-prod-edp-sa-udw/repo-prod-sa-udw-dataflow-tmplt-1/jdbc-export-progress:20261030-v1`
- 새 템플릿: `gs://bucket-prod-sa-udw-dataflow-system-1/templates/jdbc-export-progress/20261030-v1.json`
- 기존 `jdbc-export:v1`과 공용 템플릿은 이 도구의 쓰기 대상이 아닙니다.

위 approved-* 버킷은 예시 이름이며 실제 승인된 **기존 버킷/위치/권한**으로 교체해야 합니다.
`--project`, `--region`, `--repository`, `--bucket`은 명시 변경할 수 있습니다.
Cloud Build 위치는 `--build-region`으로 지정하며 미지정 시 `--region`(기본 asia-northeast3)을 명시 전달합니다.
`--gcs-source-staging-dir`은 execute에서 필수입니다. 기본 PROJECT_cloudbuild 소스 버킷으로 묵시 전환하지 않습니다.
`--gcs-log-dir`은 선택 사항이며 미지정 시 Cloud Build 기본 로그 정책/위치가 적용될 수 있습니다.
멀티리전 로그 저장도 피하려면 승인된 지역 버킷을 명시하십시오. 빌드 지역과 버킷 위치는 서로 다릅니다.
명시한 source/log 버킷은 정확한 REST GET 성공으로 존재·읽기권한을 확인하며 404도 중단합니다.
이 도구는 버킷을 이동/생성하거나 실제 버킷 location을 검사하지 않으므로 운영자가 위치를 사전에 확인해야 합니다.
이미지 이름 `jdbc-export-progress`와 템플릿 하위경로는 고정이며 임의 IMAGE/TEMPLATE_PATH를 받지 않습니다.
기존 셸에 이 두 환경변수가 설정돼 있으면 중단하므로 `unset IMAGE TEMPLATE_PATH` 후 다시 확인하십시오.
버전은 날짜접두사+고유접미사(`20261030-v1`, `20261030-023000-a1b2` 등)를 명시합니다.
재실행이나 실패 후에는 기존 상태를 확인하고 새 버전을 사용합니다. `v1`/`latest` 단독은 거부됩니다.

## 덮어쓰기 방지와 정확한 한계

1. Artifact Registry 저장소가 표준 DOCKER이며 **immutableTags=true**인지 읽기 전용으로 확인합니다.
   미설정/권한부족/네트워크/응답오류는 중단합니다. 설정을 자동 변경하지 않습니다.
   기존 저장소를 immutable로 바꾸면 다른 팀의 태그 갱신도 막으므로 **임의로 바꾸지 마십시오**.
   저장소 담당자가 승인한 별도 immutable B 저장소를 준비한 뒤 `--repository`로 지정할 수 있습니다.
   새 저장소 생성/정책변경/IAM 승인과 수행은 이 도구 범위 밖입니다.
2. 정확한 B 태그와 정확한 템플릿 객체를 REST GET으로 조회합니다. 대상 GET의 HTTP404만 부재로 인정합니다.
   401/403/429/5xx/통신오류/JSON오류를 ‘없음’으로 처리하지 않습니다. 저장소/버킷 자체도 먼저 확인합니다.
3. `flex-template build --image=... --print-only`의 stdout JSON을 로컬에서 검증합니다.
   이 명령은 GCS에 저장하지 않습니다. 새 이미지 경로/PYTHON/metadata 형태가 맞지 않으면 빌드 전 중단합니다.
4. immutable 정책을 재확인하고 Cloud Build로 새 태그를 빌드합니다. 태그 재지정 경쟁은 저장소 immutable 정책으로
   차단합니다. 운영자가 정책을 동시 해제하면 이 보장은 없어집니다. 관리자 변경까지 통제하는 잠금은 아닙니다.
   동일 digest의 중복 push 자체까지 막는 exactly-once 빌드는 아닙니다.
5. 빌드 성공 후 태그 digest 참조를 확인하고, 로컬 spec을 `gcloud storage cp --if-generation-match=0`으로 업로드합니다.
   이는 대상에 현재 live 객체가 없을 때만 생성하는 **서버 조건부 쓰기**입니다. 검사 이후 다른 실행이 먼저 만들면 실패합니다.
   삭제된 객체의 과거 세대까지 검사하는 영구 일회성 이름 보장은 아닙니다.
6. 템플릿 업로드가 실패해도 기존 객체를 삭제하거나 덮어쓰지 않습니다. 새 이미지만 남을 수 있으며 자동 정리/재시도는 없습니다.
   중단/시간초과/오류 시 결과가 일부 반영됐을 수 있으므로 Cloud Build와 정확한 두 대상을 확인하십시오.

CLI는 로그인된 gcloud 사용자의 access token을 메모리에서 읽기 조회에만 사용하며 출력/파일저장하지 않습니다.
서비스 계정 키를 만들지 않습니다. Artifact Registry repository/tag 읽기, 버킷/객체 읽기,
Cloud Build 실행과 빌드 계정의 Artifact Registry push, 실행자의 신규 GCS object 생성 권한 등이 필요합니다.
이 도구는 IAM/API enablement/정책을 변경하지 않습니다. 실제 조직별 build SA/네트워크 정책은 별도 검증해야 합니다.
stdout으로 진행 계획을 출력하며 Cloud Build 실패 시 명령/종료코드만 알립니다. 원문 오류/비밀은 출력하지 않으므로
Cloud Build 콘솔에서 실제 빌드 상태를 확인하십시오. 실패를 성공으로 간주하지 않습니다.

## 빌드 후 선택 배포 / 롤백

빌드 성공은 배포 완료가 아닙니다. 테스트 Vine의 기존 `runtime.dataflow` 나머지 설정을 보존하면서
`template_gcs_path`만 새 경로로 지정하고 신규 Job으로 먼저 검증합니다. Composer에는 executor/progress/diagnostics
의 대응 버전을 비교 병합합니다. 자동으로 Vine·운영 Job·대시보드를 변경하지 않습니다.
기존 이미지와 템플릿을 지우지 않고 보존하면, 문제가 있을 때 테스트 Vine의 template_gcs_path를 이전 값으로 돌릴 수 있습니다.
실행 중 Job은 이 변경으로 바뀌지 않습니다. JDBC fetch 설정/BigQuery 적재방식은 변경하지 않습니다.

## 검증 / 공식 근거

- `python -m unittest discover -s tests -p 'test_dataflow_safe_build.py' -v`
- dry-run / 기존대상 / mutable repository / 403·통신오류 / 잘못된 spec / generation조건 / 경로주입 회귀 테스트.
- 실제 GCP 빌드·정책·REST 응답·업로드·배포는 미실행이며 mock 테스트는 이를 대체하지 않습니다.
- https://docs.cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/build (`--print-only`)
- https://docs.cloud.google.com/sdk/gcloud/reference/storage/cp (`--if-generation-match`)
- https://docs.cloud.google.com/storage/docs/request-preconditions (generation0)
- https://docs.cloud.google.com/artifact-registry/docs/docker/names (immutable tags)
- https://docs.cloud.google.com/artifact-registry/docs/reference/rest/v1/projects.locations.repositories
- https://docs.cloud.google.com/artifact-registry/docs/reference/rest/v1/projects.locations.repositories.packages.tags/get

- https://docs.cloud.google.com/sdk/gcloud/reference/builds/submit (region/source staging/log directory)
