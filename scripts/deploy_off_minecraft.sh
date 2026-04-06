#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MOD_DIR="${ROOT_DIR}/space-logger-mod"
REMOTE_HOST="${SPACE_LOGGER_REMOTE_HOST:-off}"
REMOTE_DEPLOY_ROOT="${SPACE_LOGGER_REMOTE_DEPLOY_ROOT:-/home/dom/deploys/space-logger-mc}"
REMOTE_SERVER_DIR="${REMOTE_DEPLOY_ROOT}/server"
REMOTE_CACHE_DIR="${REMOTE_DEPLOY_ROOT}/cache"
REMOTE_UPLOADS_DIR="${REMOTE_DEPLOY_ROOT}/uploads"
REMOTE_CURRENT="${REMOTE_DEPLOY_ROOT}/current"
SERVICE_NAME="${SPACE_LOGGER_SERVICE_NAME:-space-logger-mc.service}"
FABRIC_INSTALLER_VERSION="${SPACE_LOGGER_FABRIC_INSTALLER_VERSION:-1.1.1}"
JRE_URL="${SPACE_LOGGER_LINUX_JRE_URL:-https://github.com/adoptium/temurin25-binaries/releases/download/jdk-25.0.2%2B10/OpenJDK25U-jre_x64_linux_hotspot_25.0.2_10.tar.gz}"
JRE_ARCHIVE_NAME="${SPACE_LOGGER_LINUX_JRE_ARCHIVE_NAME:-OpenJDK25U-jre_x64_linux_hotspot_25.0.2_10.tar.gz}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

prop() {
  local key="$1"
  local file="$2"
  local value
  value="$(awk -F= -v key="$key" '$1 == key { print $2 }' "$file" | tail -n 1)"
  if [[ -z "${value}" ]]; then
    echo "missing property ${key} in ${file}" >&2
    exit 1
  fi
  printf '%s' "${value}"
}

find_cached_jar() {
  local group_path="$1"
  local artifact="$2"
  local version="$3"
  local jar_name="${artifact}-${version}.jar"
  local result
  result="$(find "${HOME}/.gradle/caches/modules-2/files-2.1/${group_path}/${artifact}/${version}" -name "${jar_name}" | head -n 1 || true)"
  if [[ -z "${result}" ]]; then
    echo "failed to locate ${jar_name} in Gradle cache" >&2
    exit 1
  fi
  printf '%s' "${result}"
}

shell_quote() {
  printf '%q' "$1"
}

require_cmd ssh
require_cmd scp
require_cmd awk

GRADLE_PROPS="${MOD_DIR}/gradle.properties"
MC_VERSION="$(prop minecraft_version "${GRADLE_PROPS}")"
LOADER_VERSION="$(prop loader_version "${GRADLE_PROPS}")"
KOTLIN_LOADER_VERSION="$(prop kotlin_loader_version "${GRADLE_PROPS}")"
FABRIC_API_VERSION="$(prop fabric_version "${GRADLE_PROPS}")"
MOD_VERSION="$(prop mod_version "${GRADLE_PROPS}")"
ARCHIVES_BASE_NAME="$(prop archives_base_name "${GRADLE_PROPS}")"
MOD_JAR="${MOD_DIR}/build/libs/${ARCHIVES_BASE_NAME}-${MOD_VERSION}.jar"

echo "[1/5] build mod jar"
(cd "${ROOT_DIR}" && ./agent-gradlew jar)

if [[ ! -f "${MOD_JAR}" ]]; then
  echo "missing built mod jar: ${MOD_JAR}" >&2
  exit 1
fi

FABRIC_API_JAR="$(find_cached_jar "net.fabricmc.fabric-api" "fabric-api" "${FABRIC_API_VERSION}")"
FABRIC_KOTLIN_JAR="$(find_cached_jar "net.fabricmc" "fabric-language-kotlin" "${KOTLIN_LOADER_VERSION}")"

REMOTE_STAMP="$(ssh "${REMOTE_HOST}" 'date +%Y%m%d_%H%M%S')"
REMOTE_STAGE_DIR="${REMOTE_UPLOADS_DIR}/${REMOTE_STAMP}"

echo "[2/5] prepare remote staging"
ssh "${REMOTE_HOST}" "mkdir -p '${REMOTE_DEPLOY_ROOT}' '${REMOTE_CACHE_DIR}' '${REMOTE_UPLOADS_DIR}' '${REMOTE_STAGE_DIR}'"

echo "[3/5] upload jars"
scp \
  "${MOD_JAR}" \
  "${FABRIC_API_JAR}" \
  "${FABRIC_KOTLIN_JAR}" \
  "${REMOTE_HOST}:${REMOTE_STAGE_DIR}/"

echo "[4/5] bootstrap remote server if needed, install mods, restart service"
ssh "${REMOTE_HOST}" "$(
  cat <<EOF
REMOTE_DEPLOY_ROOT=$(shell_quote "${REMOTE_DEPLOY_ROOT}") \
REMOTE_SERVER_DIR=$(shell_quote "${REMOTE_SERVER_DIR}") \
REMOTE_CACHE_DIR=$(shell_quote "${REMOTE_CACHE_DIR}") \
REMOTE_STAGE_DIR=$(shell_quote "${REMOTE_STAGE_DIR}") \
REMOTE_CURRENT=$(shell_quote "${REMOTE_CURRENT}") \
SERVICE_NAME=$(shell_quote "${SERVICE_NAME}") \
FABRIC_INSTALLER_VERSION=$(shell_quote "${FABRIC_INSTALLER_VERSION}") \
JRE_URL=$(shell_quote "${JRE_URL}") \
JRE_ARCHIVE_NAME=$(shell_quote "${JRE_ARCHIVE_NAME}") \
MC_VERSION=$(shell_quote "${MC_VERSION}") \
LOADER_VERSION=$(shell_quote "${LOADER_VERSION}") \
ARCHIVES_BASE_NAME=$(shell_quote "${ARCHIVES_BASE_NAME}") \
bash -s
EOF
)" <<'REMOTE'
set -euo pipefail

FABRIC_INSTALLER_URL="https://maven.fabricmc.net/net/fabricmc/fabric-installer/${FABRIC_INSTALLER_VERSION}/fabric-installer-${FABRIC_INSTALLER_VERSION}.jar"
INSTALLER_JAR="${REMOTE_CACHE_DIR}/fabric-installer-${FABRIC_INSTALLER_VERSION}.jar"
JRE_ARCHIVE="${REMOTE_CACHE_DIR}/${JRE_ARCHIVE_NAME}"

remote_require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "remote missing required command: $1" >&2
    exit 1
  fi
}

remote_server_complete_at() {
  local server_dir="$1"
  [[ -d "${server_dir}" ]] \
    && [[ -f "${server_dir}/fabric-server-launch.jar" ]] \
    && [[ -d "${server_dir}/libraries" ]] \
    && [[ -d "${server_dir}/versions" ]]
}

adopt_existing_server_if_present() {
  if [[ -e "${REMOTE_SERVER_DIR}" ]]; then
    return
  fi
  if [[ -L "${REMOTE_CURRENT}" ]]; then
    local current_target
    current_target="$(readlink -f "${REMOTE_CURRENT}")"
    if [[ -n "${current_target}" ]] && remote_server_complete_at "${current_target}/server"; then
      ln -sfn "${current_target}/server" "${REMOTE_SERVER_DIR}"
    fi
  fi
}

ensure_remote_file() {
  local url="$1"
  local output="$2"
  if [[ -f "${output}" ]]; then
    return
  fi
  curl -L --fail --retry 3 -A 'Mozilla/5.0' "${url}" -o "${output}"
}

ensure_remote_jre() {
  if [[ -x "${REMOTE_SERVER_DIR}/jre/bin/java" ]]; then
    return
  fi
  ensure_remote_file "${JRE_URL}" "${JRE_ARCHIVE}"
  mkdir -p "${REMOTE_SERVER_DIR}/jre"
  tar -xzf "${JRE_ARCHIVE}" -C "${REMOTE_SERVER_DIR}/jre" --strip-components=1
}

write_start_script() {
  cat > "${REMOTE_SERVER_DIR}/start.sh" <<'SH'
#!/bin/sh
set -eu
BASE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
JAVA_BIN="$BASE_DIR/jre/bin/java"
if [ ! -x "$JAVA_BIN" ]; then
  JAVA_BIN=java
fi
exec "$JAVA_BIN" \
  -Xms1G -Xmx2G \
  -jar "$BASE_DIR/fabric-server-launch.jar" nogui
SH
  chmod +x "${REMOTE_SERVER_DIR}/start.sh"
}

write_service_unit() {
  cat > "/tmp/${SERVICE_NAME}" <<UNIT
[Unit]
Description=Space Logger Fabric Minecraft Server
After=network.target

[Service]
Type=simple
User=dom
WorkingDirectory=${REMOTE_SERVER_DIR}
ExecStart=${REMOTE_SERVER_DIR}/start.sh
Restart=on-failure
RestartSec=5
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
UNIT

  sudo -n mv "/tmp/${SERVICE_NAME}" "/etc/systemd/system/${SERVICE_NAME}"
  sudo -n systemctl daemon-reload
  sudo -n systemctl enable "${SERVICE_NAME}" >/dev/null
}

install_uploaded_mods() {
  mkdir -p "${REMOTE_SERVER_DIR}/mods"
  find "${REMOTE_SERVER_DIR}/mods" -maxdepth 1 -type f \
    \( \
      -name "${ARCHIVES_BASE_NAME}-*.jar" -o \
      -name 'fabric-api-*.jar' -o \
      -name 'fabric-language-kotlin-*.jar' \
    \) \
    -delete
  mv "${REMOTE_STAGE_DIR}"/*.jar "${REMOTE_SERVER_DIR}/mods/"
  rmdir "${REMOTE_STAGE_DIR}"
}

remote_require_cmd curl
remote_require_cmd tar
remote_require_cmd sudo
remote_require_cmd systemctl
remote_require_cmd ss

mkdir -p "${REMOTE_DEPLOY_ROOT}" "${REMOTE_CACHE_DIR}"
adopt_existing_server_if_present
if [[ ! -e "${REMOTE_SERVER_DIR}" ]]; then
  mkdir -p "${REMOTE_SERVER_DIR}"
fi

ensure_remote_jre
REMOTE_JAVA_BIN="${REMOTE_SERVER_DIR}/jre/bin/java"
if [[ ! -x "${REMOTE_JAVA_BIN}" ]]; then
  REMOTE_JAVA_BIN=java
fi

if ! remote_server_complete_at "${REMOTE_SERVER_DIR}"; then
  ensure_remote_file "${FABRIC_INSTALLER_URL}" "${INSTALLER_JAR}"
  "${REMOTE_JAVA_BIN}" -jar "${INSTALLER_JAR}" server \
    -mcversion "${MC_VERSION}" \
    -loader "${LOADER_VERSION}" \
    -downloadMinecraft \
    -dir "${REMOTE_SERVER_DIR}" >/dev/null
fi

printf 'eula=true\n' > "${REMOTE_SERVER_DIR}/eula.txt"
write_start_script
write_service_unit
install_uploaded_mods
sudo -n systemctl restart "${SERVICE_NAME}"
REMOTE

echo "[5/5] wait for remote readiness"
ssh "${REMOTE_HOST}" "$(
  cat <<EOF
REMOTE_SERVER_DIR=$(shell_quote "${REMOTE_SERVER_DIR}") \
SERVICE_NAME=$(shell_quote "${SERVICE_NAME}") \
bash -s
EOF
)" <<'REMOTE'
set -euo pipefail

LOG_FILE="${REMOTE_SERVER_DIR}/logs/latest.log"
READY=no

for _ in $(seq 1 60); do
  ACTIVE="$(systemctl is-active "${SERVICE_NAME}" || true)"
  if [[ "${ACTIVE}" != "active" ]]; then
    echo "service-not-active:${ACTIVE}" >&2
    systemctl status "${SERVICE_NAME}" --no-pager -n 60 || true
    [[ -f "${LOG_FILE}" ]] && tail -n 120 "${LOG_FILE}" || true
    exit 1
  fi

  if ss -ltn | grep -q ':25565 ' && grep -q 'Done (' "${LOG_FILE}" 2>/dev/null; then
    READY=yes
    break
  fi

  sleep 5
done

if [[ "${READY}" != "yes" ]]; then
  echo "timed-out-waiting-for-server-ready" >&2
  systemctl status "${SERVICE_NAME}" --no-pager -n 60 || true
  [[ -f "${LOG_FILE}" ]] && tail -n 120 "${LOG_FILE}" || true
  exit 1
fi

echo "remote_server=${REMOTE_SERVER_DIR}"
systemctl status "${SERVICE_NAME}" --no-pager -n 20 | sed -n '1,25p'
echo '--- listeners ---'
ss -ltn | egrep '25565' || true
echo '--- logtail ---'
tail -n 80 "${LOG_FILE}"
REMOTE
