use crate::connection::{ConnectionConfig, create_connection_ssl};
use crate::logging::Logger;
use crate::queries::{GET_PLAN_INDEXES, GET_PLAN_INDEXES_COMPAT};
use crate::schema::discover_all_user_schemas;
use crate::types::{LogFormat, PlanIndexInfo, PlanOutputFormat, SortBy, SslMode};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;

/// CLI arguments for the `plan` subcommand
#[derive(clap::Args, Debug)]
#[command(about = "Generate a ranked index worklist without modifying the database")]
pub struct PlanArgs {
    #[arg(short = 'H', long, help = "Host to connect to PostgreSQL")]
    pub host: Option<String>,

    #[arg(short, long, help = "Port to connect to PostgreSQL")]
    pub port: Option<u16>,

    #[arg(short, long, help = "Database name to connect to")]
    pub database: Option<String>,

    #[arg(short = 'U', long, help = "Username for the PostgreSQL user")]
    pub username: Option<String>,

    #[arg(short = 'P', long, help = "Password for the PostgreSQL user")]
    pub password: Option<String>,

    #[arg(
        short = 's',
        long,
        help = "Schema name(s) to plan, comma-separated. Not required if --discover-all-schemas is used."
    )]
    pub schema: Option<String>,

    #[arg(
        long,
        default_value = "false",
        help = "Discover all user schemas automatically"
    )]
    pub discover_all_schemas: bool,

    #[arg(
        long,
        default_value = "disable",
        value_parser = clap::value_parser!(SslMode),
        help = "SSL mode: disable, require, verify-ca, verify-full"
    )]
    pub sslmode: SslMode,

    #[arg(long, help = "Path to CA certificate file (.pem)")]
    pub ssl_ca_cert: Option<String>,

    #[arg(long, help = "Path to client certificate file (.pem)")]
    pub ssl_client_cert: Option<String>,

    #[arg(long, help = "Path to client private key file (.pem)")]
    pub ssl_client_key: Option<String>,

    #[arg(
        short = 'C',
        long,
        value_name = "FILE",
        help = "Path to TOML configuration file"
    )]
    pub config: Option<String>,

    #[arg(
        long,
        default_value = "bloat",
        help = "Comma-separated ranking criteria: bloat, size, scan-frequency, age"
    )]
    pub sort_by: String,

    #[arg(
        long,
        default_value = "json",
        value_parser = clap::value_parser!(PlanOutputFormat),
        help = "Output format: json or csv"
    )]
    pub format: PlanOutputFormat,

    #[arg(long, help = "Write output to file instead of stdout")]
    pub output: Option<String>,
}

/// TOML config subset recognised by the plan subcommand
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct PlanConfig {
    host: Option<String>,
    port: Option<u16>,
    database: Option<String>,
    username: Option<String>,
    password: Option<String>,
    schema: Option<String>,
    discover_all_schemas: Option<bool>,
    sslmode: Option<String>,
    ssl_ca_cert: Option<String>,
    ssl_client_cert: Option<String>,
    ssl_client_key: Option<String>,
    sort_by: Option<String>,
    format: Option<String>,
    output: Option<String>,
}

fn load_plan_config(path: &str) -> Result<PlanConfig> {
    if !Path::new(path).exists() {
        return Err(anyhow::anyhow!("Configuration file not found: {}", path));
    }
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read configuration file: {}", path))?;
    toml::from_str::<PlanConfig>(&content)
        .with_context(|| format!("Failed to parse TOML configuration file: {}", path))
}

fn merge_plan_config(cfg: PlanConfig, mut args: PlanArgs) -> PlanArgs {
    if args.host.is_none() {
        args.host = cfg.host;
    }
    if args.port.is_none() {
        args.port = cfg.port;
    }
    if args.database.is_none() {
        args.database = cfg.database;
    }
    if args.username.is_none() {
        args.username = cfg.username;
    }
    if args.password.is_none() {
        args.password = cfg.password;
    }
    if args.schema.is_none() {
        args.schema = cfg.schema;
    }
    if !args.discover_all_schemas {
        args.discover_all_schemas = cfg.discover_all_schemas.unwrap_or(false);
    }
    if args.sslmode == SslMode::Disable {
        if let Some(ref mode_str) = cfg.sslmode {
            if let Ok(mode) = mode_str.parse::<SslMode>() {
                args.sslmode = mode;
            }
        }
    }
    if args.ssl_ca_cert.is_none() {
        args.ssl_ca_cert = cfg.ssl_ca_cert;
    }
    if args.ssl_client_cert.is_none() {
        args.ssl_client_cert = cfg.ssl_client_cert;
    }
    if args.ssl_client_key.is_none() {
        args.ssl_client_key = cfg.ssl_client_key;
    }
    if args.sort_by == "bloat" {
        if let Some(s) = cfg.sort_by {
            args.sort_by = s;
        }
    }
    if args.format == PlanOutputFormat::Json {
        if let Some(fmt) = cfg.format {
            if let Ok(parsed) = fmt.parse::<PlanOutputFormat>() {
                args.format = parsed;
            }
        }
    }
    if args.output.is_none() {
        args.output = cfg.output;
    }
    args
}

fn parse_sort_criteria(sort_by: &str) -> Result<Vec<SortBy>> {
    sort_by
        .split(',')
        .map(|s| s.trim().parse::<SortBy>().map_err(|e| anyhow::anyhow!(e)))
        .collect()
}

async fn query_plan_indexes(
    client: &tokio_postgres::Client,
    schemas: &[String],
) -> Result<Vec<PlanIndexInfo>> {
    let mut use_compat = false;
    let mut all_rows: Vec<PlanIndexInfo> = Vec::new();

    for schema in schemas {
        let query = if use_compat {
            GET_PLAN_INDEXES_COMPAT
        } else {
            GET_PLAN_INDEXES
        };

        let result = client.query(query, &[schema]).await;
        let rows = match result {
            Ok(r) => r,
            Err(ref e) if e.to_string().contains("last_idx_scan") => {
                use_compat = true;
                client
                    .query(GET_PLAN_INDEXES_COMPAT, &[schema])
                    .await
                    .with_context(|| {
                        format!("Failed to query index plan data for schema '{}'", schema)
                    })?
            }
            Err(e) => {
                return Err(anyhow::anyhow!(e)).with_context(|| {
                    format!("Failed to query index plan data for schema '{}'", schema)
                })
            }
        };

        for row in rows {
            all_rows.push(PlanIndexInfo {
                rank: 0,
                schema_name: row.get(0),
                table_name: row.get(1),
                index_name: row.get(2),
                index_type: row.get(3),
                size_bytes: row.get(4),
                size_pretty: row.get(5),
                scan_count: row.get(6),
                last_scan: row.get(7),
                estimated_bloat_percent: row.get(9),
                last_idx_scan_epoch_secs: row.get(8),
            });
        }
    }

    Ok(all_rows)
}

fn metric(row: &PlanIndexInfo, criterion: SortBy, now_secs: i64) -> f64 {
    match criterion {
        SortBy::Bloat => row.estimated_bloat_percent,
        SortBy::Size => row.size_bytes as f64,
        // Fewer scans = higher priority; negate so normalization puts it at the top
        SortBy::ScanFrequency => -(row.scan_count as f64),
        // Older last scan = higher priority; None (never scanned) = highest priority
        SortBy::Age => match row.last_idx_scan_epoch_secs {
            None => f64::MAX,
            Some(epoch) => (now_secs - epoch) as f64,
        },
    }
}

fn rank_indexes(rows: Vec<PlanIndexInfo>, criteria: &[SortBy]) -> Vec<PlanIndexInfo> {
    if rows.is_empty() || criteria.is_empty() {
        return rows;
    }

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let scores: Vec<f64> = if criteria.len() == 1 {
        let c = criteria[0];
        rows.iter().map(|r| metric(r, c, now_secs)).collect()
    } else {
        let mut raw: Vec<Vec<f64>> = criteria
            .iter()
            .map(|&c| rows.iter().map(|r| metric(r, c, now_secs)).collect())
            .collect();

        // Normalize each criterion to [0, 1]; f64::MAX (never-scanned) maps to 1.0
        for values in &mut raw {
            let finite: Vec<f64> = values.iter().copied().filter(|v| v.is_finite()).collect();
            let min = finite.iter().cloned().fold(f64::MAX, f64::min);
            let max = finite.iter().cloned().fold(f64::MIN, f64::max);
            let range = max - min;
            for v in values.iter_mut() {
                *v = if !v.is_finite() {
                    1.0
                } else if range == 0.0 {
                    0.0
                } else {
                    (*v - min) / range
                };
            }
        }

        (0..rows.len())
            .map(|i| raw.iter().map(|vals| vals[i]).sum())
            .collect()
    };

    let mut pairs: Vec<(f64, PlanIndexInfo)> = scores.into_iter().zip(rows).collect();
    pairs.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    pairs
        .into_iter()
        .enumerate()
        .map(|(i, (_, mut row))| {
            row.rank = i + 1;
            row
        })
        .collect()
}

fn csv_quote(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn output_json(ranked: &[PlanIndexInfo], dest: &mut dyn Write) -> Result<()> {
    let json = serde_json::to_string_pretty(ranked).context("Failed to serialize plan to JSON")?;
    writeln!(dest, "{}", json)?;
    Ok(())
}

fn output_csv(ranked: &[PlanIndexInfo], dest: &mut dyn Write) -> Result<()> {
    writeln!(
        dest,
        "rank,schema_name,table_name,index_name,index_type,size_bytes,size_pretty,scan_count,last_scan,estimated_bloat_percent"
    )?;
    for row in ranked {
        let last_scan = row.last_scan.as_deref().map(csv_quote).unwrap_or_default();
        writeln!(
            dest,
            "{},{},{},{},{},{},{},{},{},{:.2}",
            row.rank,
            csv_quote(&row.schema_name),
            csv_quote(&row.table_name),
            csv_quote(&row.index_name),
            csv_quote(&row.index_type),
            row.size_bytes,
            csv_quote(&row.size_pretty),
            row.scan_count,
            last_scan,
            row.estimated_bloat_percent,
        )?;
    }
    Ok(())
}

/// Entry point for the `plan` subcommand
pub async fn run_plan(mut args: PlanArgs) -> Result<()> {
    if let Some(config_path) = args.config.clone() {
        let cfg = load_plan_config(&config_path)?;
        args = merge_plan_config(cfg, args);
    }

    if args.schema.is_none() && !args.discover_all_schemas {
        return Err(anyhow::anyhow!(
            "Either --schema or --discover-all-schemas must be provided for the plan subcommand"
        ));
    }

    let criteria = parse_sort_criteria(&args.sort_by)?;

    let conn_cfg = ConnectionConfig::from_args(
        args.host.clone(),
        args.port,
        args.database.clone(),
        args.username.clone(),
        args.password.clone(),
        args.sslmode,
        args.ssl_ca_cert.clone(),
        args.ssl_client_cert.clone(),
        args.ssl_client_key.clone(),
    )?;
    let conn_str = conn_cfg.build_connection_string();

    // Logger passed to create_connection_ssl; silenced so it doesn't pollute plan output
    let logger = Logger::new("/dev/null".to_string(), true, LogFormat::Text);

    let client = create_connection_ssl(
        &conn_str,
        &conn_cfg.sslmode,
        conn_cfg.ssl_ca_cert.clone(),
        conn_cfg.ssl_client_cert.clone(),
        conn_cfg.ssl_client_key.clone(),
        &logger,
    )
    .await?;

    let schemas: Vec<String> = if let Some(schema_str) = &args.schema {
        schema_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        discover_all_user_schemas(&client).await?
    };

    if schemas.is_empty() {
        return Err(anyhow::anyhow!("No schemas found to plan"));
    }

    let rows = query_plan_indexes(&client, &schemas).await?;
    let ranked = rank_indexes(rows, &criteria);

    match &args.output {
        Some(path) => {
            let file = std::fs::File::create(path)
                .with_context(|| format!("Failed to create output file: {}", path))?;
            let mut writer = std::io::BufWriter::new(file);
            match args.format {
                PlanOutputFormat::Json => output_json(&ranked, &mut writer),
                PlanOutputFormat::Csv => output_csv(&ranked, &mut writer),
            }
        }
        None => {
            let stdout = std::io::stdout();
            let mut locked = stdout.lock();
            match args.format {
                PlanOutputFormat::Json => output_json(&ranked, &mut locked),
                PlanOutputFormat::Csv => output_csv(&ranked, &mut locked),
            }
        }
    }
}
