#!/usr/bin/env bash
# Wrapper to call the archiver per model/element on a daily UTC window.
# Special hours:
#   - nbm, nbm_exp: START=YYYYmmdd0100, END=(YYYYmmdd+1)0000
#   - others:       START=YYYYmmdd0000, END=YYYYmmdd2300
#
# Usage:
#   ./run_daily_archives.sh                 # uses yesterday UTC
#   ./run_daily_archives.sh 2025-11-02      # run for a specific UTC date
#   ./run_daily_archives.sh --start 202511010100 --end 202511020000  # override
#
# Env:
#   DRY_RUN=1
#   LOG_DIR=/path
#   MODELS="nbm hrrr"

set -Eeuo pipefail

declare -A AVAILABLE_FIELDS=(
  [nbm]="Wind snow6hr snow24hr snow48hr snow72hr"
  [nbm_exp]="snow6hr snow24hr snow48hr snow72hr"
  [nbmqmd]="precip24hr precip6hr maxt mint"
  [nbmqmd_exp]="precip24hr precip6hr maxt mint Wind Gust"
  [hrrr]="Wind precip6hr snow6hr"
  [urma]="Wind"
)

ts() { date -u +"%Y-%m-%d %H:%M:%S UTC"; }
log_info()  { echo "[$(ts)] [INFO ] $*"; }
log_warn()  { echo "[$(ts)] [WARN ] $*" >&2; }
log_error() { echo "[$(ts)] [ERROR] $*" >&2; }

START_ARG=""
END_ARG=""
DATE_ARG="${1-}"

if [[ "${1-}" == "--start" ]]; then
  START_ARG="${2-}"; shift 2 || true
  if [[ "${1-}" == "--end" ]]; then
    END_ARG="${2-}"; shift 2 || true
  else
    log_error "When using --start you must also supply --end."
    exit 2
  fi
elif [[ -n "${DATE_ARG}" && "${DATE_ARG}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  :
else
  DATE_ARG=""
fi

# Determine base UTC date (the "day" we’re running)
if [[ -n "${START_ARG}" && -n "${END_ARG}" ]]; then
  # Explicit override; we'll pass through as-is for all models
  GLOBAL_START="${START_ARG}"
  GLOBAL_END="${END_ARG}"
else
  if [[ -n "${DATE_ARG}" ]]; then
    BASE_DATE="${DATE_ARG}"
  else
    BASE_DATE="$(date -u -d "yesterday" +%F)"
  fi
fi

LOG_DIR="${LOG_DIR:-./logs}"
mkdir -p "$LOG_DIR"
RUN_STAMP="$(date -u +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/archive_${RUN_STAMP}.log"

log_info "Mode: UTC. Base date: ${BASE_DATE:-explicit}  (log: $LOG_FILE)" | tee -a "$LOG_FILE"

# Allow narrowing the model set
if [[ -n "${MODELS:-}" ]]; then
  read -r -a MODEL_LIST <<< "${MODELS}"
else
  MODEL_LIST=("${!AVAILABLE_FIELDS[@]}")
fi

RC=0
for model in "${MODEL_LIST[@]}"; do
  if [[ -z "${AVAILABLE_FIELDS[$model]+set}" ]]; then
    log_warn "Model '${model}' not in AVAILABLE_FIELDS; skipping." | tee -a "$LOG_FILE"
    continue
  fi

  IFS=' ' read -r -a elements <<< "${AVAILABLE_FIELDS[$model]}"

  for element in "${elements[@]}"; do
    # Compute START/END per model
    if [[ -n "${GLOBAL_START:-}" && -n "${GLOBAL_END:-}" ]]; then
      START="${GLOBAL_START}"
      END="${GLOBAL_END}"
    else
      if [[ "$model" == "nbm" || "$model" == "nbm_exp" ]]; then
        START="$(date -u -d "${BASE_DATE} 01:00" +%Y%m%d%H)00"
        END="$(date -u -d "${BASE_DATE} +1 day 01:00" +%Y%m%d%H)00"
      else
        START="$(date -u -d "${BASE_DATE} 00:00" +%Y%m%d%H)00"
        END="$(date -u -d "${BASE_DATE} +1 day 00:00" +%Y%m%d%H)00"
      fi
    fi

    cmd=( python run_model_archiver.py
          --start "${START}"
          --end "${END}"
          --model "${model}"
          --element "${element}"
          --local )

    if [[ "${DRY_RUN:-0}" == "1" ]]; then
      log_info "DRY_RUN: ${cmd[*]}" | tee -a "$LOG_FILE"
      continue
    fi

    log_info "Running: ${cmd[*]}" | tee -a "$LOG_FILE"
    if "${cmd[@]}" >>"$LOG_FILE" 2>&1; then
      log_info "OK: model=${model} element=${element} (START=${START} END=${END})" | tee -a "$LOG_FILE"
    else
      log_error "FAILED: model=${model} element=${element} (START=${START} END=${END})" | tee -a "$LOG_FILE"
      RC=1
    fi
  done
done

exit "$RC"
