package cql;

import classes.Column;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.cassandraunit.AbstractCassandraUnit4CQLTestCase;
import org.cassandraunit.dataset.CQLDataSet;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;

public class CqlTest extends AbstractCassandraUnit4CQLTestCase {

    public CQLDataSet getDataSet() {
        return new ClassPathCQLDataSet("youtube.cql");
    }

    @Test
    public void isYouTubeKeySpaceCreated() {
        ResultSet result = getSession().execute("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'youtube';");
        assertThat(result.all(), hasSize(1));
    }

    @Test
    public void isVideosTableCreated() {
        ResultSet result = getSession().execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='youtube' AND table_name='videos';");
        List<Row> rows = result.all();
        assertThat(rows, hasSize(1));
    }

    @Test
    public void videosTableHasCorrectColumns() {
        ResultSet result = getSession().execute("SELECT * FROM system_schema.columns WHERE keyspace_name = 'youtube' AND table_name = 'videos';");
        List<Column> columns = Column.fromResultSet(result);

        assertThat(columns, hasSize(5));
        assertThat(columns, containsInAnyOrder(
                new Column("video_id", "none", "partition_key", "uuid"),
                new Column("title", "none", "regular", "text"),
                new Column("description", "none", "regular", "text"),
                new Column("tags", "none", "regular", "set<text>"),
                new Column("upload_date", "none", "regular", "date")
        ));
    }
}