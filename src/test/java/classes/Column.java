package classes;

import com.datastax.driver.core.ResultSet;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Column {
    private String column_name;
    private String clustering_order;
    private String kind;
    private String type;

    public Column(String column_name, String clustering_order, String kind, String type) {
        this.column_name = column_name;
        this.clustering_order = clustering_order;
        this.kind = kind;
        this.type = type;
    }

    public static List<Column> fromResultSet(ResultSet resultSet) {
        return resultSet.all().stream()
                .map(row -> new Column(
                        row.getString("column_name"),
                        row.getString("clustering_order"),
                        row.getString("kind"),
                        row.getString("type")))
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        Column column = (Column) obj;

        return Objects.equals(column_name, column.column_name)
                && Objects.equals(clustering_order, column.clustering_order)
                && Objects.equals(kind, column.kind)
                && Objects.equals(type, column.type);
    }
}