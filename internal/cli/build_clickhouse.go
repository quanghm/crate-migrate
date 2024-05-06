//go:build clickhouse
// +build clickhouse

package cli

import (
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/quanghm/crate-migrate/v4/database/clickhouse"
)
