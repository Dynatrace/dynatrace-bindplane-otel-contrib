module github.com/dynatrace/dynatrace-bindplane-otel-contrib/exporter/exabeamexporter

go 1.26.10

require (
	github.com/aws/aws-sdk-go-v2 v1.47.1
	github.com/aws/aws-sdk-go-v2/credentials v1.20.6
	github.com/aws/aws-sdk-go-v2/service/s3 v1.114.0
	github.com/aws/smithy-go v1.28.1
	github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/testutils v1.1.0
	github.com/dynatrace/dynatrace-bindplane-otel-contrib/pkg/expr v1.1.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl v0.162.0
	github.com/stretchr/testify v1.12.1
	go.opentelemetry.io/collector/component v1.68.0
	go.opentelemetry.io/collector/component/componenttest v0.162.0
	go.opentelemetry.io/collector/config/configopaque v1.68.0
	go.opentelemetry.io/collector/config/configoptional v1.68.0
	go.opentelemetry.io/collector/config/configretry v1.68.0
	go.opentelemetry.io/collector/confmap v1.68.0
	go.opentelemetry.io/collector/consumer v1.68.0
	go.opentelemetry.io/collector/consumer/consumererror v0.162.0
	go.opentelemetry.io/collector/exporter v1.68.0
	go.opentelemetry.io/collector/exporter/exporterhelper v0.162.0
	go.opentelemetry.io/collector/exporter/exportertest v0.162.0
	go.opentelemetry.io/collector/pdata v1.68.0
	go.opentelemetry.io/otel v1.46.0
	go.opentelemetry.io/otel/metric v1.46.0
	go.opentelemetry.io/otel/sdk/metric v1.46.0
	go.opentelemetry.io/otel/trace v1.46.0
	go.uber.org/goleak v1.3.0
	go.uber.org/zap v1.28.0
)

require (
	github.com/alecthomas/participle/v2 v2.1.4 // indirect
	github.com/antchfx/xmlquery v1.5.1 // indirect
	github.com/antchfx/xpath v1.3.8 // indirect
	github.com/elastic/go-grok v0.3.1 // indirect
	github.com/elastic/lunes v0.2.2 // indirect
	github.com/expr-lang/expr v1.17.8 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/iancoleman/strcase v0.3.0 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/magefile/mage v1.15.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.162.0 // indirect
	github.com/twmb/murmur3 v1.2.0 // indirect
	github.com/ua-parser/uap-go v0.0.0-20251207011819-db9adb27a0b8 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20260218203240-3dfff04db8fa // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.11.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.20.4 // indirect
	github.com/cenkalti/backoff/v7 v7.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/go-version v1.9.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/knadh/koanf/maps v0.1.3 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.1 // indirect
	github.com/knadh/koanf/v2 v2.3.8 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector/client v1.68.0 // indirect
	go.opentelemetry.io/collector/consumer/consumertest v0.162.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.162.0 // indirect
	go.opentelemetry.io/collector/exporter/xexporter v0.162.0 // indirect
	go.opentelemetry.io/collector/extension v1.68.0 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.162.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.68.0 // indirect
	go.opentelemetry.io/collector/internal/componentalias v0.162.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.162.0 // indirect
	go.opentelemetry.io/collector/pdata/xpdata v0.162.0 // indirect
	go.opentelemetry.io/collector/pipeline v1.68.0 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.162.0 // indirect
	go.opentelemetry.io/collector/receiver v1.68.0 // indirect
	go.opentelemetry.io/collector/receiver/receivertest v0.162.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.162.0 // indirect
	go.opentelemetry.io/otel/sdk v1.46.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/sys v0.48.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/grpc v1.84.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/dynatrace/dynatrace-bindplane-otel-contrib/pkg/expr => ../../pkg/expr

replace github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/testutils => ../../internal/testutils
