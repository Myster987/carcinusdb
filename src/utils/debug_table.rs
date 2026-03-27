use std::fmt::{Debug, Display};

/// Helper to print table-like data.
pub struct DebugTable {
    column_names: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl DebugTable {
    pub fn new() -> Self {
        Self {
            column_names: vec![],
            rows: vec![],
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn add_column(&mut self, name: &str) {
        self.column_names.push(name.to_string());
    }

    pub fn insert_row(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }
}

impl Display for DebugTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let widths: Vec<usize> = self
            .column_names
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let max_val = self
                    .rows
                    .iter()
                    .map(|row| row.get(i).map(|s| s.len()).unwrap_or(0))
                    .max()
                    .unwrap_or(0);
                col.len().max(max_val)
            })
            .collect();

        let separator = |f: &mut std::fmt::Formatter| -> std::fmt::Result {
            write!(f, "+")?;
            for w in &widths {
                write!(f, "{:-<width$}+", "", width = w + 2)?;
            }
            writeln!(f)
        };

        let write_row = |f: &mut std::fmt::Formatter, values: &[String]| -> std::fmt::Result {
            write!(f, "|")?;
            for (i, w) in widths.iter().enumerate() {
                let val = values.get(i).map(|s| s.as_str()).unwrap_or("");
                write!(f, " {:<width$} |", val, width = w)?;
            }
            writeln!(f)
        };

        separator(f)?;
        write_row(f, &self.column_names)?;
        separator(f)?;
        for row in &self.rows {
            write_row(f, row)?;
        }
        separator(f)?;
        write!(f, "({} rows)", self.rows.len())
    }
}

impl Debug for DebugTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}
