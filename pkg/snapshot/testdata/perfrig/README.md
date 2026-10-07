# Snapshot processor A/B rig

Measures the whole-collector cost of the snapshot processors: two collectors
from the same image run side by side on one host under identical load, one
with the three snapshot processor instances Bindplane renders into every
pipeline and one without. Results and interpretation live in
[`../../PERFORMANCE.md`](../../PERFORMANCE.md); this directory holds what is
needed to run it again.

## Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | `with-snapshot` and `no-snapshot` collectors from `${IMAGE}`, `${CPUS:-2}` CPUs each, config mounted at `/etc/otel/config.yaml`, pprof on 1777, self-telemetry on 8888 |
| `configs/batch1/{with,no}.yaml` | otlp → `snapshotprocessor/_s0_source` → `snapshotprocessor` → `snapshotprocessor/_d0_dest` → nop, and the same without snapshot processors |
| `configs/batch1000/{with,no}.yaml` | as above with a `batch` processor (1,000 records) first in both |
| `run.sh` | one scenario for one image: 90 s of telemetrygen load into both collectors at once, docker stats sampling, per-record CPU and allocation from the collectors' own metrics, 30 s CPU profile and heap profile from the with-snapshot side |
| `analyze.sh` | extracts the cum % of the snapshot-path symbols from a saved profile |
| `Dockerfile.patched` | the released `ghcr.io/dynatrace/dynatrace-bindplane-otel-collector:1.0.0` image with its binary replaced by a locally compiled one (`--build-arg BIN=<file>`), so an A/B run differs only in collector code |
| `telemetrygen/Dockerfile` | `FROM scratch` image around a cross-compiled telemetrygen binary, for hosts that cannot pull the upstream image |
| `remote-setup.sh` | Docker install, image builds and architecture checks for a fresh Ubuntu VM (copy the directory and the collector binaries to the VM, run it once, then `run.sh` as above) |

## Quick start (local Docker)

```sh
# 1. telemetrygen image (once)
GOOS=linux GOARCH=$(go env GOARCH) go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@v0.139.0
cp "$(go env GOPATH)"/bin/*/telemetrygen telemetrygen/ 2>/dev/null || cp "$(go env GOPATH)/bin/telemetrygen" telemetrygen/
docker build -t telemetrygen-local:v0.139.0 telemetrygen

# 2. a collector image from a local build of dynatrace-bindplane-otel-collector
#    (a sibling checkout by default; point COLLECTOR elsewhere if needed)
COLLECTOR=${COLLECTOR:-../../../../../dynatrace-bindplane-otel-collector}
(cd "$COLLECTOR" && make install-ocb && GOOS=linux GOARCH=$(go env GOARCH) make agent)
cp "$COLLECTOR/dist/collector_linux_$(go env GOARCH)" ./collector_bin
docker build -t bdot-patched:mybuild --build-arg BIN=collector_bin -f Dockerfile.patched .

# 3. run; results land in results/<scenario>-<tag>/
IMAGE=ghcr.io/dynatrace/dynatrace-bindplane-otel-collector:1.0.0 ./run.sh batch1
IMAGE=bdot-patched:mybuild ./run.sh batch1
IMAGE=bdot-patched:mybuild ./run.sh batch1000
```

`run.sh` prints CPU µs per record and allocation KB per record for both sides;
compare `with` against `without` within one run, never absolute numbers across
hosts. The two containers are loaded separately, so differences under about
3 % are run-to-run noise.
