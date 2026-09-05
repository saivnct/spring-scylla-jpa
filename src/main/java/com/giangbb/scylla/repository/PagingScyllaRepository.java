package com.giangbb.scylla.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.paging.OffsetPager;
import com.giangbb.scylla.core.ScyllaTemplate;
import org.springframework.data.domain.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Created by Giangbb on 15/07/2025
 */
public class PagingScyllaRepository<T> extends SimpleScyllaRepository<T> {
    private static final CqlIdentifier APPLIED = CqlIdentifier.fromInternal("[applied]");


    public PagingScyllaRepository(Class<T> tClass, ScyllaTemplate scyllaTemplate) {
        super(tClass, scyllaTemplate);
    }

    /**
     * The offset-paging counterpart of {@code PagingScyllaRepository.findSliceWithPageAndOffset}, over
     * a caller-supplied row mapper.
     *
     * @param boundStatement Query statement
     * @param pageable Pageable
     * @param mapper Function<Row, R>
     * @return PageModel with page info and content
     */
    protected <R> Slice<R> findSliceWithPageAndOffset(BoundStatement boundStatement, Pageable pageable, Function<Row, R> mapper) {
        //align the server page size with the logical page size
        BoundStatement stmt = boundStatement.setPageSize(pageable.getPageSize());

        ResultSet rs = this.execute(stmt);

        OffsetPager pager = new OffsetPager(pageable.getPageSize());
        OffsetPager.Page<Row> pageRow = pager.getPage(rs, pageable.getPageNumber() + 1);

        List<R> content = new ArrayList<>(pageRow.getElements().size());
        for (Row row : pageRow.getElements()) {
            if (row != null && (row.getColumnDefinitions().size() != 1 || !row.getColumnDefinitions().get(0).getName().equals(APPLIED))){
                R mapped = mapper.apply(row);
                if (mapped != null) {
                    content.add(mapped);
                }
            }
        }

        return new SliceImpl<>(content, pageable, !pageRow.isLast());
    }


    /**
     * Paging query
     * @param boundStatement Query statement
     * @param pageable Pageable
     * @return PageModel with page info and content
     */
    protected Slice<T> findSliceWithPageAndOffset(BoundStatement boundStatement, Pageable pageable) {
        return findSliceWithPageAndOffset(boundStatement, pageable, this.getSingleRowMapper());
    }
}
