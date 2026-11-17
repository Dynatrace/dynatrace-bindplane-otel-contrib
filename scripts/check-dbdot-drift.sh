#!/usr/bin/env bash
# Copyright  Dynatrace LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Compares internal/dbdot/manifest.yaml against the DBDOT collector's manifest.
#
# Usage: check-dbdot-drift.sh [COLLECTOR_MANIFEST]
#   COLLECTOR_MANIFEST is a local path or URL; defaults to the collector's main branch.
#
# Exits 1 on structural drift: a component or replace present in one manifest
# but not the other. Version differences only produce warnings, since contrib's
# manifest legitimately runs ahead of the collector during an OTel bump.
#
# Ignored on purpose:
#   - collector-internal modules (github.com/dynatrace/dynatrace-bindplane-otel-collector/...),
#     which the replica omits;
#   - contrib module versions, which the build replaces with the local tree.

set -euo pipefail

REPLICA=internal/dbdot/manifest.yaml
SOURCE=${1:-https://raw.githubusercontent.com/Dynatrace/dynatrace-bindplane-otel-collector/main/manifests/dynatrace-bindplane-otel-collector/manifest.yaml}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

case "$SOURCE" in
http://* | https://*) curl -fsSL --retry 3 --retry-delay 5 "$SOURCE" -o "$tmp/source.yaml" ;;
*) cp "$SOURCE" "$tmp/source.yaml" ;;
esac

# Emit one "<section>\t<module>\t<version-or-target>" line per component/replace.
normalize() {
    awk '
    /^[a-z_]+:/ { section = $1; sub(/:$/, "", section); next }
    /^[[:space:]]*#/ { next }
    section == "replaces" && /^[[:space:]]*- / {
        line = $0
        sub(/^[[:space:]]*- /, "", line); sub(/[[:space:]]+$/, "", line)
        n = index(line, " => ")
        if (n == 0) next
        mod = substr(line, 1, n - 1); val = substr(line, n + 4)
        emit(section, mod, val); next
    }
    /^[[:space:]]*- gomod: / { emit(section, $3, $4) }
    function emit(s, m, v) {
        if (m ~ /^github\.com\/dynatrace\/dynatrace-bindplane-otel-collector\//) return
        if (m ~ /^github\.com\/dynatrace\/dynatrace-bindplane-otel-contrib\//) v = "-"
        printf "%s\t%s\t%s\n", s, m, v
    }
    ' "$1" | sort
}

normalize "$REPLICA" > "$tmp/replica.tsv"
normalize "$tmp/source.yaml" > "$tmp/source.tsv"

cut -f1,2 "$tmp/replica.tsv" > "$tmp/replica.keys"
cut -f1,2 "$tmp/source.tsv" > "$tmp/source.keys"

status=0

missing=$(comm -13 "$tmp/replica.keys" "$tmp/source.keys")
extra=$(comm -23 "$tmp/replica.keys" "$tmp/source.keys")
if [ -n "$missing" ] || [ -n "$extra" ]; then
    status=1
    echo "Structural drift between $REPLICA and the collector manifest:"
    [ -n "$missing" ] && printf '%s\n' "$missing" | sed 's/^/  missing from replica: /'
    [ -n "$extra" ] && printf '%s\n' "$extra" | sed 's/^/  not in collector:     /'
fi

# Version differences for entries present in both.
versions=$(join -t $'\t' \
    <(awk -F'\t' '{print $1 "|" $2 "\t" $3}' "$tmp/replica.tsv" | sort) \
    <(awk -F'\t' '{print $1 "|" $2 "\t" $3}' "$tmp/source.tsv" | sort) |
    awk -F'\t' '$2 != $3 { split($1, k, "|"); printf "  %s %s: replica %s, collector %s\n", k[1], k[2], $2, $3 }')
if [ -n "$versions" ]; then
    echo "Version differences (expected while an OTel bump is in flight):"
    printf '%s\n' "$versions"
    if [ -n "${GITHUB_ACTIONS:-}" ]; then
        echo "::warning title=DBDOT drift::$(printf '%s\n' "$versions" | wc -l | tr -d ' ') version difference(s) between the replica and collector manifests; see the job log."
    fi
fi

if [ "$status" -eq 0 ] && [ -z "$versions" ]; then
    echo "$REPLICA matches the collector manifest."
fi
exit "$status"
