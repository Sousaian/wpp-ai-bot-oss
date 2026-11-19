#!/bin/sh
set -eu

# Resolve only this Compose project's containers, never globally fixed names.
privacy_container_id() {
    privacy_id=$(docker compose ps -q "$1")
    [ -n "$privacy_id" ] || { printf 'nginx_privacy_probe=missing_service service=%s\n' "$1" >&2; exit 1; }
    printf '%s' "$privacy_id"
}
privacy_app=$(privacy_container_id app)
privacy_nginx=$(privacy_container_id nginx)

privacy_base_url="${PRIVACY_BASE_URL:-http://127.0.0.1}"
privacy_host="${PRIVACY_HOST:-localhost}"
privacy_phone="1555$(date +%s | cut -c 3-10)73"
privacy_country=$(printf '%s' "$privacy_phone" | cut -c 1-2)
privacy_area=$(printf '%s' "$privacy_phone" | cut -c 3-4)
privacy_prefix=$(printf '%s' "$privacy_phone" | cut -c 5-9)
privacy_suffix=$(printf '%s' "$privacy_phone" | cut -c 10-14)
privacy_formatted="+$privacy_country ($privacy_area) $privacy_prefix-$privacy_suffix"
privacy_encoded="%2B$privacy_country%20%28$privacy_area%29%20$privacy_prefix-$privacy_suffix"
privacy_single_encoded_phone=$(printf '%s' "$privacy_phone" \
    | od -An -tx1 \
    | awk '{ for (i = 1; i <= NF; i++) printf "%%%s", toupper($i) }')
privacy_double_encoded_phone=$(printf '%s' "$privacy_phone" \
    | od -An -tx1 \
    | awk '{ for (i = 1; i <= NF; i++) printf "%%25%s", toupper($i) }')
privacy_single_encoded_formatted=$(printf '%s' "$privacy_formatted" \
    | od -An -tx1 \
    | awk '{ for (i = 1; i <= NF; i++) printf "%%%s", toupper($i) }')
privacy_double_encoded_formatted=$(printf '%s' "$privacy_formatted" \
    | od -An -tx1 \
    | awk '{ for (i = 1; i <= NF; i++) printf "%%25%s", toupper($i) }')

privacy_first_digit=$(printf '%s' "$privacy_phone" | cut -c 1)
privacy_middle_digit=$(printf '%s' "$privacy_phone" | cut -c 7)
privacy_last_digit=$(printf '%s' "$privacy_phone" | cut -c 14)
privacy_first_hex=$(printf '%s' "$privacy_first_digit" | od -An -tx1 | awk '{ printf "%s", toupper($1) }')
privacy_middle_hex=$(printf '%s' "$privacy_middle_digit" | od -An -tx1 | awk '{ printf "%s", toupper($1) }')
privacy_last_hex=$(printf '%s' "$privacy_last_digit" | od -An -tx1 | awk '{ printf "%s", toupper($1) }')
privacy_mixed_first="%25$privacy_first_hex$(printf '%s' "$privacy_phone" | cut -c 2-14)"
privacy_mixed_middle="$(printf '%s' "$privacy_phone" | cut -c 1-6)%25$privacy_middle_hex$(printf '%s' "$privacy_phone" | cut -c 8-14)"
privacy_mixed_last="$(printf '%s' "$privacy_phone" | cut -c 1-13)%25$privacy_last_hex"
privacy_mixed_first_normalized="%$privacy_first_hex$(printf '%s' "$privacy_phone" | cut -c 2-14)"
privacy_mixed_middle_normalized="$(printf '%s' "$privacy_phone" | cut -c 1-6)%$privacy_middle_hex$(printf '%s' "$privacy_phone" | cut -c 8-14)"
privacy_mixed_last_normalized="$(printf '%s' "$privacy_phone" | cut -c 1-13)%$privacy_last_hex"

privacy_resource_id="9876$(date +%s | cut -c 4-9)"
privacy_unknown_token="privacy-unknown-$(date +%s)-shape"
privacy_since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
privacy_app_stopped=0

privacy_restore_app() {
    if [ "$privacy_app_stopped" -eq 1 ]; then
        docker start "$privacy_app" >/dev/null 2>&1 || true
    fi
}
trap privacy_restore_app EXIT INT TERM

privacy_request() {
    privacy_request_path=$1
    privacy_query_separator="?"
    case "$privacy_request_path" in *\?*) privacy_query_separator="&" ;; esac
    curl -g -sS --max-time 15 -o /dev/null -w '%{http_code}' \
        -H "Host: $privacy_host" \
        -H "Referer: https://example.invalid/$privacy_phone" \
        -H "User-Agent: privacy-probe/$privacy_formatted" \
        "$privacy_base_url$privacy_request_path${privacy_query_separator}q=$privacy_encoded&handler=bot&status=active&page=2&include_contacts=true" \
        2>/dev/null
}

privacy_raw_status=$(privacy_request "/sessoes/$privacy_phone" || true)
privacy_formatted_status=$(privacy_request "/sessoes/$privacy_encoded" || true)
privacy_single_digits_status=$(privacy_request "/sessoes/$privacy_single_encoded_phone" || true)
privacy_single_formatted_status=$(privacy_request "/sessoes/$privacy_single_encoded_formatted" || true)
privacy_double_digits_status=$(privacy_request "/sessoes/$privacy_double_encoded_phone" || true)
privacy_double_formatted_status=$(privacy_request "/sessoes/$privacy_double_encoded_formatted" || true)
privacy_mixed_first_status=$(privacy_request "/sessoes/$privacy_mixed_first" || true)
privacy_mixed_middle_status=$(privacy_request "/sessoes/$privacy_mixed_middle" || true)
privacy_mixed_last_status=$(privacy_request "/sessoes/$privacy_mixed_last" || true)
privacy_resource_status=$(privacy_request "/agentes/$privacy_resource_id" || true)
privacy_unknown_status=$(privacy_request "/$privacy_unknown_token/$privacy_phone" || true)

for privacy_routed_status in \
    "$privacy_raw_status" \
    "$privacy_formatted_status" \
    "$privacy_single_digits_status" \
    "$privacy_single_formatted_status"
do
    if [ "$privacy_routed_status" != "404" ]; then
        printf 'nginx_privacy_probe=failed_normalized_route status=%s\n' \
            "$privacy_routed_status"
        exit 1
    fi
done

for privacy_rejected_status in \
    "$privacy_double_digits_status" \
    "$privacy_double_formatted_status" \
    "$privacy_mixed_first_status" \
    "$privacy_mixed_middle_status" \
    "$privacy_mixed_last_status"
do
    if [ "$privacy_rejected_status" != "400" ]; then
        printf 'nginx_privacy_probe=failed_encoded_digit_rejection status=%s\n' \
            "$privacy_rejected_status"
        exit 1
    fi
done

if [ "$privacy_resource_status" != "404" ] || [ "$privacy_unknown_status" != "404" ]; then
    printf 'nginx_privacy_probe=failed_route_shape_probe\n'
    exit 1
fi

if [ "$(docker inspect -f '{{.State.Running}}' "$privacy_app" 2>/dev/null)" != "true" ]; then
    printf 'nginx_privacy_probe=failed_app_not_running\n'
    exit 1
fi

docker stop "$privacy_app" >/dev/null
privacy_app_stopped=1
# Let Nginx's documented 10-second Docker DNS cache expire so the controlled
# failure exercises the resolver-unavailable 502 path deterministically.
sleep 11
privacy_failure_status=$(privacy_request "/health?phone=$privacy_phone" || true)
if [ "$privacy_failure_status" != "502" ]; then
    printf 'nginx_privacy_probe=failed_controlled_upstream_status status=%s\n' \
        "$privacy_failure_status"
    exit 1
fi

docker start "$privacy_app" >/dev/null
privacy_app_stopped=0
privacy_recovery_status=""
privacy_recovery_attempt=0
while [ "$privacy_recovery_attempt" -lt 30 ]; do
    privacy_recovery_status=$(curl -g -sS --max-time 3 -o /dev/null -w '%{http_code}' \
        -H "Host: $privacy_host" "$privacy_base_url/" 2>/dev/null || true)
    if [ "$privacy_recovery_status" = "200" ]; then
        break
    fi
    privacy_recovery_attempt=$((privacy_recovery_attempt + 1))
    sleep 1
done
if [ "$privacy_recovery_status" != "200" ]; then
    printf 'nginx_privacy_probe=failed_app_recovery\n'
    exit 1
fi

# Docker preserves the container's stdout/stderr split: Nginx access logs are
# captured from stdout and error logs from stderr so each privacy surface is
# checked independently.
privacy_nginx_access_logs=$(docker logs --since "$privacy_since" "$privacy_nginx" 2>/dev/null)
privacy_nginx_error_logs=$(docker logs --since "$privacy_since" "$privacy_nginx" 2>&1 >/dev/null)

for privacy_log_surface in "$privacy_nginx_access_logs" "$privacy_nginx_error_logs"; do
    for privacy_forbidden in \
        "$privacy_phone" \
        "$privacy_formatted" \
        "$privacy_encoded" \
        "$privacy_single_encoded_phone" \
        "$privacy_double_encoded_phone" \
        "$privacy_single_encoded_formatted" \
        "$privacy_double_encoded_formatted" \
        "$privacy_mixed_first" \
        "$privacy_mixed_middle" \
        "$privacy_mixed_last" \
        "$privacy_mixed_first_normalized" \
        "$privacy_mixed_middle_normalized" \
        "$privacy_mixed_last_normalized" \
        "$privacy_resource_id" \
        "$privacy_unknown_token"
    do
        if printf '%s' "$privacy_log_surface" | rg -F -i -q "$privacy_forbidden"; then
            printf 'nginx_privacy_probe=failed_nginx_dynamic_path_log\n'
            exit 1
        fi
    done
done

for privacy_service in app chatwoot chatwoot-sidekiq evolution; do
    privacy_container=$(privacy_container_id "$privacy_service")
    privacy_container_logs=$(docker logs --since "$privacy_since" "$privacy_container" 2>&1)
    privacy_forbidden_index=0
    for privacy_forbidden in \
        "$privacy_phone" \
        "$privacy_formatted" \
        "$privacy_single_encoded_phone" \
        "$privacy_double_encoded_phone" \
        "$privacy_single_encoded_formatted" \
        "$privacy_double_encoded_formatted" \
        "$privacy_mixed_first" \
        "$privacy_mixed_middle" \
        "$privacy_mixed_last" \
        "$privacy_mixed_first_normalized" \
        "$privacy_mixed_middle_normalized" \
        "$privacy_mixed_last_normalized"
    do
        privacy_forbidden_index=$((privacy_forbidden_index + 1))
        if printf '%s' "$privacy_container_logs" | rg -F -i -q "$privacy_forbidden"; then
            printf 'nginx_privacy_probe=failed_cross_container_phone_log container=%s marker=%s\n' \
                "$privacy_container" "$privacy_forbidden_index"
            exit 1
        fi
    done
done

privacy_session_shapes=$(printf '%s' "$privacy_nginx_access_logs" \
    | rg -F -c 'path="/sessoes/[REDACTED_PHONE]"' || true)
privacy_rejected_shapes=$(printf '%s' "$privacy_nginx_access_logs" \
    | rg -F 'path="/sessoes/[REDACTED_PHONE]"' \
    | rg -c 'status=400' || true)
if [ "$privacy_session_shapes" -lt 9 ] || [ "$privacy_rejected_shapes" -lt 5 ]; then
    printf 'nginx_privacy_probe=failed_static_session_shape\n'
    exit 1
fi

for privacy_expected in \
    'method="GET"' \
    'path="/sessoes/[REDACTED_PHONE]"' \
    'path="/agentes/[REDACTED_ID]"' \
    'path="[REDACTED_PATH]"' \
    'query_handler="bot"' \
    'query_status="active"' \
    'query_page="2"' \
    'query_include_contacts="true"' \
    'query_q="[REDACTED_PHONE]"' \
    'status=' \
    'request_time=' \
    'upstream_status=' \
    'upstream_connect_time=' \
    'upstream_response_time='
do
    if ! printf '%s' "$privacy_nginx_access_logs" | rg -F -q "$privacy_expected"; then
        printf 'nginx_privacy_probe=failed_safe_metadata\n'
        exit 1
    fi
done

if ! printf '%s' "$privacy_nginx_access_logs" \
    | rg -F 'path="/health"' \
    | rg -q 'status=502 .*request_time=.* upstream_status='; then
    printf 'nginx_privacy_probe=failed_safe_upstream_evidence\n'
    exit 1
fi

printf 'nginx_privacy_probe=clean raw=%s formatted=%s single_digits=%s single_formatted=%s double_digits=%s double_formatted=%s mixed_first=%s mixed_middle=%s mixed_last=%s failure=%s recovery=%s\n' \
    "$privacy_raw_status" \
    "$privacy_formatted_status" \
    "$privacy_single_digits_status" \
    "$privacy_single_formatted_status" \
    "$privacy_double_digits_status" \
    "$privacy_double_formatted_status" \
    "$privacy_mixed_first_status" \
    "$privacy_mixed_middle_status" \
    "$privacy_mixed_last_status" \
    "$privacy_failure_status" \
    "$privacy_recovery_status"
