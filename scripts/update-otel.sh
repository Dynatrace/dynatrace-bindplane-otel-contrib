#!/bin/sh
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

# This is the list of stable (v1.0.0+) modules in the core repository.
read -d '' STABLE_MODULES <<EOF
go.opentelemetry.io/collector/client
go.opentelemetry.io/collector/featuregate
go.opentelemetry.io/collector/pdata
go.opentelemetry.io/collector/component
go.opentelemetry.io/collector/confmap
go.opentelemetry.io/collector/confmap/provider/envprovider
go.opentelemetry.io/collector/confmap/provider/fileprovider
go.opentelemetry.io/collector/confmap/provider/httpprovider
go.opentelemetry.io/collector/confmap/provider/httpsprovider
go.opentelemetry.io/collector/confmap/provider/yamlprovider
go.opentelemetry.io/collector/config/configopaque
go.opentelemetry.io/collector/config/configcompression
go.opentelemetry.io/collector/config/configretry
go.opentelemetry.io/collector/config/configtls
go.opentelemetry.io/collector/config/confignet
go.opentelemetry.io/collector/config/configmiddleware
go.opentelemetry.io/collector/config/configoptional
go.opentelemetry.io/collector/consumer
go.opentelemetry.io/collector/exporter
go.opentelemetry.io/collector/extension
go.opentelemetry.io/collector/extension/extensionauth
go.opentelemetry.io/collector/pipeline
go.opentelemetry.io/collector/processor
go.opentelemetry.io/collector/receiver
EOF

# This is the list of contrib modules that have reached v1.0.0+ and are versioned
# independently of the main contrib release (CONTRIB_TARGET_VERSION). They are
# pinned to CONTRIB_STABLE_TARGET_VERSION instead. Keep this in sync with the same
# list in update-docs.sh. When a new contrib module goes stable, add its full
# module path here.
read -d '' STABLE_CONTRIB_MODULES <<EOF
github.com/open-telemetry/opentelemetry-collector-contrib/processor/k8sattributesprocessor
github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl
EOF

# Exit on any error (set after read command which returns non-zero at EOF)
set -e

is_stable_module() {
    for stable_mod in $STABLE_MODULES; do
        if [ "$stable_mod" = "$1" ]; then
            return 0
        fi
    done
    return 1
}

is_stable_contrib_module() {
    for stable_mod in $STABLE_CONTRIB_MODULES; do
        if [ "$stable_mod" = "$1" ]; then
            return 0
        fi
    done
    return 1
}

is_contrib_module() {
    case $1 in
    github.com/open-telemetry/opentelemetry-collector-contrib*)
        return 0
        ;;
    esac
    return 1
}

TARGET_VERSION=$1
if [ -z "$TARGET_VERSION" ]; then
    echo "Must specify a target version"
    exit 1
fi

CONTRIB_TARGET_VERSION=$2
if [ -z "$CONTRIB_TARGET_VERSION" ]; then
    echo "Must specify a target contrib version"
    exit 1
fi

PDATA_TARGET_VERSION=$3

if [ -z "$PDATA_TARGET_VERSION" ]; then
    echo "Must specify a target pdata version"
    exit 1
fi

CONTRIB_STABLE_TARGET_VERSION=$4
if [ -z "$CONTRIB_STABLE_TARGET_VERSION" ]; then
    echo "Must specify a target stable contrib version"
    exit 1
fi

# Bump OTel versions in the DBDOT replica manifest used by the build check.
# Only entries on the current OTel minor (and stable v1 modules) are bumped, so
# deliberate pins to older versions are left alone. Fork redirects in the
# manifest's replaces section and non-OTel components must be updated by hand.
DBDOT_MANIFEST=internal/dbdot/manifest.yaml
if [ -f "$DBDOT_MANIFEST" ]; then
    CURRENT_MINOR=$(grep -oE 'go\.opentelemetry\.io/collector/[^ ]+ v0\.[0-9]+\.' "$DBDOT_MANIFEST" | head -1 | sed -E 's/.* v0\.([0-9]+)\./\1/')
    echo "Updating $DBDOT_MANIFEST (current OTel minor: v0.$CURRENT_MINOR)"
    awk \
        -v minor="$CURRENT_MINOR" \
        -v core="$TARGET_VERSION" \
        -v contrib="$CONTRIB_TARGET_VERSION" \
        -v pdata="$PDATA_TARGET_VERSION" \
        -v contrib_stable="$CONTRIB_STABLE_TARGET_VERSION" \
        -v stable_list="$(echo $STABLE_MODULES)" \
        -v stable_contrib_list="$(echo $STABLE_CONTRIB_MODULES)" \
        '
        BEGIN {
            n = split(stable_list, a, " "); for (i = 1; i <= n; i++) stable[a[i]] = 1
            n = split(stable_contrib_list, a, " "); for (i = 1; i <= n; i++) stable_contrib[a[i]] = 1
        }
        $1 == "-" && $2 == "gomod:" && NF == 4 {
            mod = $3; ver = $4; new = ""
            current = (index(ver, "v0." minor ".") == 1)
            if (mod in stable) new = pdata
            else if (mod in stable_contrib) new = contrib_stable
            else if (mod ~ /^github\.com\/open-telemetry\/opentelemetry-collector-contrib\// && current) new = contrib
            else if (mod ~ /^go\.opentelemetry\.io\/collector\// && current) new = core
            if (new != "") { sub(/[ \t]+$/, ""); $0 = substr($0, 1, length($0) - length(ver)) new }
        }
        { print }
        ' "$DBDOT_MANIFEST" > "$DBDOT_MANIFEST.tmp"
    mv "$DBDOT_MANIFEST.tmp" "$DBDOT_MANIFEST"
fi

# ocb's version tracks the OTel core version.
awk -v v="$TARGET_VERSION" '/^OCB_VERSION \?= /{print "OCB_VERSION ?= " v; next} {print}' Makefile > Makefile.tmp
mv Makefile.tmp Makefile

LOCAL_MODULES=$(find . -type f -name "go.mod" -not -path "./build/*" -exec dirname {} \; | sort)
for local_mod in $LOCAL_MODULES; do
    # Run in a subshell so that the CD doesn't change this shell's current directory
    # Temporarily disable 'set -e' for this command so we can check its exit status
    set +e
    (
        # Exit subshell on any error
        set -e

        echo "Updating deps in $local_mod"
        cd "$local_mod"
        # go list will not work if module is not tidy, so we tidy first
        GO_COMPAT=$(grep '^go ' go.mod | awk '{print $2}')
        go mod tidy -compat="$GO_COMPAT"

        echo "  Tidied $local_mod"

        # Temporarily disable 'set -e' for this command in case there are no OTEL modules
        set +e
        OTEL_MODULES=$(
            go list -m -f '{{if not (or .Indirect .Main)}}{{.Path}}{{end}}' all |
            grep -E -e '(?:^github.com/open-telemetry/opentelemetry-collector-contrib)|(?:^go.opentelemetry.io/collector)'
        )
        set -e

        for mod in $OTEL_MODULES; do
            if is_stable_module "$mod"; then
                echo "  Updating $local_mod: $mod@$PDATA_TARGET_VERSION"
                go mod edit -require "$mod@$PDATA_TARGET_VERSION"
            elif is_stable_contrib_module "$mod"; then
                echo "  Updating $local_mod: $mod@$CONTRIB_STABLE_TARGET_VERSION"
                go mod edit -require "$mod@$CONTRIB_STABLE_TARGET_VERSION"
            elif is_contrib_module "$mod"; then
                echo "  Updating $local_mod: $mod@$CONTRIB_TARGET_VERSION"
                go mod edit -require "$mod@$CONTRIB_TARGET_VERSION"
            else
                echo "  Updating $local_mod: $mod@$TARGET_VERSION"
                go mod edit -require "$mod@$TARGET_VERSION"
            fi
        done
    )
    # Get the exit status of the subshell and re-enable 'set -e'
    SUBSHELL_EXIT=$?
    set -e
    # Check if subshell failed and exit if it did
    if [ $SUBSHELL_EXIT -ne 0 ]; then
        echo "Error: Failed to update $local_mod" >&2
        exit 1
    fi
done
