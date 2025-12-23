use std::{
    collections::VecDeque,
    fmt::{Debug, Write},
};

fn trim_if_to_long(value: String, len: usize) -> String {
    let mut s = String::new();

    if value.chars().count() > len {
        let mut chars = value.chars();
        for _ in 0..len - 3 {
            s.push(chars.next().unwrap());
        }
        s.push_str("...");
        s
    } else {
        value
    }
}

pub struct DebugTable<'a> {
    column_names: Vec<&'a dyn Debug>,
    rows: Vec<Vec<&'a dyn Debug>>,
}

impl<'a> DebugTable<'a> {
    pub fn new() -> Self {
        Self {
            column_names: vec![],
            rows: vec![],
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn add_column(&mut self, name: &'a dyn Debug) {
        self.column_names.push(name.to_owned());
    }

    pub fn insert_row(&mut self, row: Vec<&'a dyn Debug>) {
        assert!(
            row.len() == self.column_names.len(),
            "row length doesn't match number of columns"
        );

        self.rows.push(row);
    }

    fn column_len(&self, index: usize) -> usize {
        format!("{:?}", self.column_names[index]).chars().count()
    }

    fn columns_width(&self) -> VecDeque<usize> {
        (0..self.column_names.len())
            .map(|i| self.column_len(i) + 2)
            .collect()
    }

    fn separator(&self) -> String {
        let mut separator = String::new();

        let mut columns = self.columns_width();

        separator.push('+');
        while let Some(len) = columns.pop_front() {
            separator.push_str(&"-".repeat(len));
            separator.push('+');
        }

        separator
    }

    fn print_header(&self) -> String {
        let mut print_row = String::new();
        let row = &self.column_names;

        let mut columns = self.columns_width();

        print_row.push('|');

        let mut i = 0;
        while let Some(len) = columns.pop_front() {
            print_row.push_str(&format!(
                " {} ",
                trim_if_to_long(format!("{:?}", row[i]), len)
            ));
            print_row.push('|');
            i += 1;
        }

        print_row
    }

    fn printable_row(&self, index: usize) -> String {
        let mut print_row = String::new();
        let row = &self.rows[index];

        let mut columns = self.columns_width();

        print_row.push('|');

        let mut i = 0;
        while let Some(len) = columns.pop_front() {
            let l = len - 2;
            let cell = trim_if_to_long(format!("{:?}", row[i]), l);

            print_row.push_str(&format!(" {:^l$} ", cell));
            print_row.push('|');
            i += 1;
        }

        print_row
    }
}

impl<'a> Debug for DebugTable<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.separator())?;
        f.write_char('\n')?;
        f.write_str(&self.print_header())?;
        f.write_char('\n')?;
        f.write_str(&self.separator())?;
        f.write_char('\n')?;

        for i in 0..self.len() {
            f.write_str(&self.printable_row(i))?;
            f.write_char('\n')?;
            f.write_str(&self.separator())?;
            f.write_char('\n')?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug_table() -> anyhow::Result<()> {
        let mut dbg_table = DebugTable::new();

        dbg_table.add_column(&"column 1");
        dbg_table.add_column(&"column 2");
        dbg_table.add_column(&"column 3");

        dbg_table.insert_row(vec![&1, &2, &3]);
        dbg_table.insert_row(vec![&1, &2, &3]);
        dbg_table.insert_row(vec![&123456789, &2, &3]);

        println!("{:?}", dbg_table);

        Ok(())
    }
}
