use crate::transpiler::traits::SqlGenerator;

pub(crate) fn split_table_reference(reference: &str) -> Option<(&str, Option<&str>)> {
    let parts = reference.split_whitespace().collect::<Vec<_>>();
    match parts.as_slice() {
        [] => None,
        [table] => Some((table, None)),
        [table, alias] => Some((table, Some(alias))),
        [table, as_keyword, alias] if as_keyword.eq_ignore_ascii_case("as") => {
            Some((table, Some(alias)))
        }
        _ => None,
    }
}

pub(crate) fn render_table_reference(reference: &str, generator: &dyn SqlGenerator) -> String {
    match split_table_reference(reference) {
        Some((table, Some(alias))) => format!(
            "{} {}",
            generator.quote_identifier(table),
            generator.quote_identifier(alias)
        ),
        Some((table, None)) => generator.quote_identifier(table),
        None => generator.quote_identifier(reference),
    }
}

pub(crate) fn table_reference_base(reference: &str) -> &str {
    split_table_reference(reference)
        .map(|(table, _)| table)
        .unwrap_or(reference)
}

pub(crate) fn table_reference_sql_qualifier(reference: &str) -> Option<&str> {
    split_table_reference(reference).map(|(table, alias)| alias.unwrap_or(table))
}

pub(crate) fn qualifier_for_column_reference<'a>(
    reference: &'a str,
    qualifier: &str,
) -> Option<&'a str> {
    let (table, alias) = split_table_reference(reference)?;
    if alias.is_some_and(|alias| alias == qualifier) || table == qualifier {
        Some(alias.unwrap_or(table))
    } else {
        None
    }
}
