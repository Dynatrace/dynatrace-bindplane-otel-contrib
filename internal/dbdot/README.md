# DBDOT replica

`manifest.yaml` is a copy of the [DBDOT collector](https://github.com/Dynatrace/dynatrace-bindplane-otel-collector)'s
ocb manifest (`manifests/dynatrace-bindplane-otel-collector/manifest.yaml`). The
build check uses it to confirm that the contrib components in this working tree
compile together with the rest of DBDOT's components and dependency pins, without
checking out the collector repo.

## Building

```bash
make build-collector                      # host GOOS/GOARCH
make build-linux                          # every linux arch (likewise build-darwin, build-windows)
```

`_build-setup` writes `build/manifest.local.yaml`: this manifest plus a `replace`
for every contrib module, pointing at its directory in this repo. ocb generates the
sources into `build/dbdot/`, and `go build` writes `build/collector_<os>_<arch>`.

The manifest pulls in private Dynatrace modules (`dt-otelcol-edge-processing-components`),
so your git credentials need read access to those repos. The Makefile sets
`GOPRIVATE=github.com/Dynatrace/*`.

## Differences from the collector manifest

- `opampconnectionextension` and its `pkg/report` replace are left out. They live
  in the collector repo and depend on contrib modules, so including them would tie
  this build to the collector repo again. The binary uses ocb's generated `main.go`
  in place of the collector's managed-mode entry point.
- `dist.module` and `dist.output_path` point at `build/dbdot` in this repo.

Everything else, including fork redirects and version pins in `replaces`, should
match the collector manifest.

## Keeping it in sync

- **OTel bumps:** `make update-otel` bumps every OTel component on the current
  minor version, the stable `v1` modules, and `OCB_VERSION`. Entries deliberately
  pinned to an older minor (such as `jmxreceiver`) are left alone. Update the
  fork redirects in `replaces` and the private Dynatrace components by hand.
- **Component or pin changes in the collector:** when the collector manifest
  adds or drops a component or changes a `replaces` entry, make the same change
  here. Then update the collector commit noted at the top of `manifest.yaml`.
- **Contrib module versions** in the manifest don't affect the build, because
  every contrib module is replaced with the local tree.

## Drift check

`scripts/check-dbdot-drift.sh` compares this manifest with the collector's
manifest on `main` (or a local path or URL passed as the first argument). The
`DBDOT Manifest Drift` workflow runs it on every PR and on demand.

- **Fails** when a component or `replaces` entry exists in one manifest but
  not the other.
- **Warns** when an entry's version or replace target differs. These are
  expected while an OTel bump is in flight, but a lasting difference in a
  fork or pin `replaces` entry should be synced.
- **Ignores** the collector-internal modules the replica omits and contrib
  module versions.

```bash
./scripts/check-dbdot-drift.sh                                  # against collector main
./scripts/check-dbdot-drift.sh ../dynatrace-bindplane-otel-collector/manifests/dynatrace-bindplane-otel-collector/manifest.yaml
```
