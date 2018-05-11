package classes;

import java.util.Objects;

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