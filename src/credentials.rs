/// Credentials module for password retrieval from .pgpass file

use anyhow::{Context, Result};
use std::{env, fs, path::Path};

/// Get password from .pgpass file
/// 
/// Searches for a matching entry in the .pgpass file based on connection parameters.
/// The .pgpass file format is: hostname:port:database:username:password
/// 
/// Wildcards (empty string or '*') match any value for that field.
/// 
/// # Arguments
/// 
/// * `host` - PostgreSQL host
/// * `port` - PostgreSQL port
/// * `database` - Database name
/// * `username` - Username
/// 
/// # Returns
/// 
/// Returns `Ok(Some(password))` if a matching entry is found, `Ok(None)` if no match is found,
/// or an error if the file cannot be read.
pub fn get_password_from_pgpass(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
) -> Result<Option<String>> {
    // Get the path to .pgpass file
    let pgpass_path = env::var("PGPASSFILE").unwrap_or_else(|_| {
        let home = env::var("HOME").unwrap_or_else(|_| env::var("USERPROFILE").unwrap_or_default());
        format!("{}/.pgpass", home)
    });

    let pgpass_path = Path::new(&pgpass_path);

    if !pgpass_path.exists() {
        return Ok(None);
    }

    // Read the .pgpass file
    let content = fs::read_to_string(pgpass_path).context("Failed to read .pgpass file")?;

    // Parse each line
    for line in content.lines() {
        let line = line.trim();

        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Split by unescaped colons (format: hostname:port:database:username:password)
        // PostgreSQL's .pgpass allows \: for literal colons and \\ for literal backslashes
        let parts = split_pgpass_line(line);
        if parts.len() != 5 {
            continue; // Skip malformed lines
        }

        let file_host = &parts[0];
        let file_port = &parts[1];
        let file_database = &parts[2];
        let file_username = &parts[3];
        let file_password = &parts[4];

        // Check if this line matches our connection parameters
        // Use wildcard matching (empty or '*' means match any)
        let host_matches = file_host.is_empty() || file_host == "*" || file_host == host;
        let port_matches =
            file_port.is_empty() || file_port == "*" || file_port == &port.to_string();
        let database_matches =
            file_database.is_empty() || file_database == "*" || file_database == database;
        let username_matches =
            file_username.is_empty() || file_username == "*" || file_username == username;

        if host_matches && port_matches && database_matches && username_matches {
            return Ok(Some(file_password.clone()));
        }
    }

    Ok(None)
}

/// Split a .pgpass line by unescaped colons, handling \: and \\ escape sequences.
fn split_pgpass_line(line: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.peek() {
                Some(':') => {
                    chars.next();
                    current.push(':');
                }
                Some('\\') => {
                    chars.next();
                    current.push('\\');
                }
                _ => {
                    current.push('\\');
                }
            }
        } else if ch == ':' {
            parts.push(std::mem::take(&mut current));
        } else {
            current.push(ch);
        }
    }
    parts.push(current);
    parts
}

