#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MOD_DIR="${ROOT_DIR}/space-logger-mod"
NATIVE_DIR="${MOD_DIR}/native-logger"
CACHE_DIR="${SPACE_LOGGER_DEPLOY_CACHE_DIR:-${HOME}/.cache/space-logger-deploy}"
REMOTE_HOST="${SPACE_LOGGER_REMOTE_HOST:-off}"
REMOTE_DEPLOY_ROOT="${SPACE_LOGGER_REMOTE_DEPLOY_ROOT:-/home/dom/deploys/space-logger-mc}"
REMOTE_RELEASES_DIR="${REMOTE_DEPLOY_ROOT}/releases"
REMOTE_CURRENT="${REMOTE_DEPLOY_ROOT}/current"
SERVICE_NAME="${SPACE_LOGGER_SERVICE_NAME:-space-logger-mc.service}"
FABRIC_INSTALLER_VERSION="${SPACE_LOGGER_FABRIC_INSTALLER_VERSION:-1.1.1}"
JRE_URL="${SPACE_LOGGER_LINUX_JRE_URL:-https://github.com/adoptium/temurin25-binaries/releases/download/jdk-25.0.2%2B10/OpenJDK25U-jre_x64_linux_hotspot_25.0.2_10.tar.gz}"
JRE_ARCHIVE_NAME="${SPACE_LOGGER_LINUX_JRE_ARCHIVE_NAME:-OpenJDK25U-jre_x64_linux_hotspot_25.0.2_10.tar.gz}"
ZIG_BIN="${SPACE_LOGGER_ZIG_BIN:-$(command -v zig || true)}"

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

require_cmd ssh
require_cmd scp
require_cmd curl
require_cmd tar
require_cmd python3

if [[ -z "${ZIG_BIN}" ]]; then
  echo "zig is required for linux x86_64 native builds" >&2
  exit 1
fi

GRADLE_PROPS="${MOD_DIR}/gradle.properties"
MC_VERSION="$(prop minecraft_version "${GRADLE_PROPS}")"
LOADER_VERSION="$(prop loader_version "${GRADLE_PROPS}")"
KOTLIN_LOADER_VERSION="$(prop kotlin_loader_version "${GRADLE_PROPS}")"
FABRIC_API_VERSION="$(prop fabric_version "${GRADLE_PROPS}")"
MOD_VERSION="$(prop mod_version "${GRADLE_PROPS}")"
ARCHIVES_BASE_NAME="$(prop archives_base_name "${GRADLE_PROPS}")"
MOD_JAR="${MOD_DIR}/build/libs/${ARCHIVES_BASE_NAME}-${MOD_VERSION}.jar"

FABRIC_API_JAR="$(find_cached_jar "net.fabricmc.fabric-api" "fabric-api" "${FABRIC_API_VERSION}")"
FABRIC_KOTLIN_JAR="$(find_cached_jar "net.fabricmc" "fabric-language-kotlin" "${KOTLIN_LOADER_VERSION}")"

mkdir -p "${CACHE_DIR}"

INSTALLER_JAR="${CACHE_DIR}/fabric-installer-${FABRIC_INSTALLER_VERSION}.jar"
if [[ ! -f "${INSTALLER_JAR}" ]]; then
  curl -L --fail --retry 3 \
    "https://maven.fabricmc.net/net/fabricmc/fabric-installer/${FABRIC_INSTALLER_VERSION}/fabric-installer-${FABRIC_INSTALLER_VERSION}.jar" \
    -o "${INSTALLER_JAR}"
fi

JRE_ARCHIVE="${CACHE_DIR}/${JRE_ARCHIVE_NAME}"
if [[ ! -f "${JRE_ARCHIVE}" ]]; then
  curl -L --fail --retry 3 -A 'Mozilla/5.0' "${JRE_URL}" -o "${JRE_ARCHIVE}"
fi

echo "[1/6] build mod jar"
(cd "${ROOT_DIR}" && ./agent-gradlew build)

echo "[2/6] build linux native library"
TMP_LINKER="$(mktemp "${TMPDIR:-/tmp}/space-logger-zig-linker.XXXXXX.sh")"
trap 'rm -f "${TMP_LINKER}"' EXIT
cat > "${TMP_LINKER}" <<EOF
#!/bin/sh
exec "${ZIG_BIN}" cc -target x86_64-linux-gnu "\$@"
EOF
chmod +x "${TMP_LINKER}"
(cd "${NATIVE_DIR}" && CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="${TMP_LINKER}" cargo build --release --target x86_64-unknown-linux-gnu)
NATIVE_SO="${NATIVE_DIR}/target/x86_64-unknown-linux-gnu/release/libspace_logger_native.so"

echo "[3/6] assemble local server bundle"
LOCAL_WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/space-logger-off-deploy.XXXXXX")"
LOCAL_SERVER_DIR="${LOCAL_WORKDIR}/server"
mkdir -p "${LOCAL_SERVER_DIR}"
java -jar "${INSTALLER_JAR}" server -mcversion "${MC_VERSION}" -loader "${LOADER_VERSION}" -downloadMinecraft -dir "${LOCAL_SERVER_DIR}" >/dev/null
mkdir -p "${LOCAL_SERVER_DIR}/mods" "${LOCAL_SERVER_DIR}/natives" "${LOCAL_SERVER_DIR}/jre"
cp "${MOD_JAR}" "${LOCAL_SERVER_DIR}/mods/"
cp "${FABRIC_API_JAR}" "${LOCAL_SERVER_DIR}/mods/"
cp "${FABRIC_KOTLIN_JAR}" "${LOCAL_SERVER_DIR}/mods/"
cp "${NATIVE_SO}" "${LOCAL_SERVER_DIR}/natives/"
printf 'eula=true\n' > "${LOCAL_SERVER_DIR}/eula.txt"
tar -xzf "${JRE_ARCHIVE}" -C "${LOCAL_SERVER_DIR}/jre" --strip-components=1
cat > "${LOCAL_SERVER_DIR}/start.sh" <<'SH'
#!/bin/sh
set -eu
BASE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
JAVA_BIN="$BASE_DIR/jre/bin/java"
if [ ! -x "$JAVA_BIN" ]; then
  JAVA_BIN=java
fi
exec "$JAVA_BIN" \
  -Xms1G -Xmx2G \
  -Dspace_logger_native_lib="$BASE_DIR/natives/libspace_logger_native.so" \
  -jar "$BASE_DIR/fabric-server-launch.jar" nogui
SH
chmod +x "${LOCAL_SERVER_DIR}/start.sh"

BUNDLE_TAR="${LOCAL_WORKDIR}/bundle.tar.gz"
tar -C "${LOCAL_WORKDIR}" -czf "${BUNDLE_TAR}" server

echo "[4/6] upload bundle to remote"
REMOTE_STAMP="$(ssh "${REMOTE_HOST}" 'date +%Y%m%d_%H%M%S')"
REMOTE_RELEASE_DIR="${REMOTE_RELEASES_DIR}/${REMOTE_STAMP}"
ssh "${REMOTE_HOST}" "mkdir -p '${REMOTE_RELEASE_DIR}' '${REMOTE_RELEASES_DIR}' '${REMOTE_DEPLOY_ROOT}'"
scp "${BUNDLE_TAR}" "${REMOTE_HOST}:${REMOTE_RELEASE_DIR}/bundle.tar.gz"

echo "[5/6] switch release and restart remote service"
cat <<REMOTE | ssh "${REMOTE_HOST}" 'bash -s'
set -euo pipefail
REMOTE_DEPLOY_ROOT='${REMOTE_DEPLOY_ROOT}'
REMOTE_RELEASE_DIR='${REMOTE_RELEASE_DIR}'
REMOTE_CURRENT='${REMOTE_CURRENT}'
SERVICE_NAME='${SERVICE_NAME}'

mkdir -p "\${REMOTE_DEPLOY_ROOT}" "\${REMOTE_RELEASE_DIR}"
cd "\${REMOTE_RELEASE_DIR}"
tar -xzf bundle.tar.gz
rm -f bundle.tar.gz

if [[ -L "\${REMOTE_CURRENT}" && -d "\$(readlink -f "\${REMOTE_CURRENT}")/server" ]]; then
  PREV_SERVER="\$(readlink -f "\${REMOTE_CURRENT}")/server"
  for path in \
    world \
    world_nether \
    world_the_end \
    server.properties \
    eula.txt \
    ops.json \
    usercache.json \
    whitelist.json \
    banned-ips.json \
    banned-players.json
  do
    if [[ -e "\${PREV_SERVER}/\${path}" && ! -e "\${REMOTE_RELEASE_DIR}/server/\${path}" ]]; then
      cp -a "\${PREV_SERVER}/\${path}" "\${REMOTE_RELEASE_DIR}/server/\${path}"
    fi
  done
fi

cat > /tmp/\${SERVICE_NAME} <<UNIT
[Unit]
Description=Space Logger Fabric Minecraft Server
After=network.target

[Service]
Type=simple
User=dom
WorkingDirectory=\${REMOTE_CURRENT}/server
ExecStart=\${REMOTE_CURRENT}/server/start.sh
Restart=on-failure
RestartSec=5
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
UNIT

sudo -n mv /tmp/\${SERVICE_NAME} /etc/systemd/system/\${SERVICE_NAME}
ln -sfn "\${REMOTE_RELEASE_DIR}" "\${REMOTE_CURRENT}"
sudo -n systemctl daemon-reload
sudo -n systemctl enable "\${SERVICE_NAME}" >/dev/null
sudo -n systemctl restart "\${SERVICE_NAME}"
REMOTE

echo "[6/6] wait for remote readiness"
cat <<REMOTE | ssh "${REMOTE_HOST}" 'bash -s'
set -euo pipefail
SERVICE_NAME='${SERVICE_NAME}'
LOG_FILE='${REMOTE_CURRENT}/server/logs/latest.log'
READY=no

for _ in \$(seq 1 60); do
  ACTIVE="\$(systemctl is-active "${SERVICE_NAME}" || true)"
  if [[ "\${ACTIVE}" != "active" ]]; then
    echo "service-not-active:\${ACTIVE}" >&2
    systemctl status "${SERVICE_NAME}" --no-pager -n 60 || true
    [[ -f "\${LOG_FILE}" ]] && tail -n 120 "\${LOG_FILE}" || true
    exit 1
  fi

  if ss -ltn | grep -q ':25565 ' && grep -q 'Done (' "\${LOG_FILE}" 2>/dev/null; then
    READY=yes
    break
  fi

  sleep 5
done

if [[ "\${READY}" != "yes" ]]; then
  echo "timed-out-waiting-for-server-ready" >&2
  systemctl status "${SERVICE_NAME}" --no-pager -n 60 || true
  [[ -f "\${LOG_FILE}" ]] && tail -n 120 "\${LOG_FILE}" || true
  exit 1
fi

echo "remote_release=${REMOTE_RELEASE_DIR}"
systemctl status "${SERVICE_NAME}" --no-pager -n 20 | sed -n '1,25p'
echo '--- listeners ---'
ss -ltn | egrep '25565' || true
echo '--- logtail ---'
tail -n 80 "\${LOG_FILE}"
REMOTE
