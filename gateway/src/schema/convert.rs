use qail_core::migrate::{Column as QailColumn, Table as QailTable};

use super::{GatewayColumn, GatewayForeignKey, GatewayTable};

pub(super) fn convert_table(table: &QailTable) -> GatewayTable {
    let columns: Vec<GatewayColumn> = table.columns.iter().map(convert_column).collect();
    let primary_key = columns
        .iter()
        .find(|c| c.primary_key)
        .map(|c| c.name.clone());

    GatewayTable {
        name: table.name.clone(),
        columns,
        primary_key,
    }
}

fn convert_column(col: &QailColumn) -> GatewayColumn {
    GatewayColumn {
        name: col.name.clone(),
        col_type: col.data_type.name().to_string(),
        pg_type: col.data_type.to_pg_type(),
        nullable: col.nullable,
        primary_key: col.primary_key,
        unique: col.unique,
        has_default: col.default.is_some() || col.generated.is_some(),
        foreign_key: col.foreign_key.as_ref().map(|fk| GatewayForeignKey {
            ref_table: fk.table.clone(),
            ref_column: fk.column.clone(),
        }),
    }
}
