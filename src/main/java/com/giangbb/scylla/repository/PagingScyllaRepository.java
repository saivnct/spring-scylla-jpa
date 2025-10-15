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
     * Paging query
     * @param boundStatement Query statement
     * @param pageable Pageable
     * @return PageModel with page info and content
     */
//    protected Page<T> findWithPageAndOffset(BoundStatement boundStatement, Pageable pageable){
//        OffsetPager pager = new OffsetPager(pageable.getPageSize());
//
//        ResultSet rs = execute(boundStatement);
//        int total = rs.getAvailableWithoutFetching();
//
//        int totalPages = pageable.getPageSize() == 0 ? 1 : (int) Math.ceil((double) total / (double) pageable.getPageSize());
//
////        log.debug("Total: {}, Page: {}, Size: {}, totalPages: {}", total, pageable.getPageNumber(), pageable.getPageSize(), totalPages);
//
//        if (pageable.getPageNumber() >= totalPages) {
//            return new PageImpl<>(new ArrayList<>(), pageable, total);
//        }
//
//        OffsetPager.Page<Row> pageRow = pager.getPage(rs, pageable.getPageNumber() + 1);
//
//        final List<T> res = new ArrayList<>();
//
//        pageRow.getElements().forEach(row -> {
//            T t = asEntity(row, this.getSingleRowMapper());
//            res.add(t);
//        });
//
////        log.debug("res: {}", res.size());
//
//        return new PageImpl<>(res, pageable, total);
//    }


    /**
     * Paging query
     * @param boundStatement Query statement
     * @param pageable Pageable
     * @return PageModel with page info and content
     */
    protected Slice<T> findSliceWithPageAndOffset(BoundStatement boundStatement, Pageable pageable) {
        // Align server page size with your logical page size
        BoundStatement stmt = boundStatement.setPageSize(pageable.getPageSize());

        ResultSet rs = execute(stmt);

        OffsetPager pager = new OffsetPager(pageable.getPageSize());
        OffsetPager.Page<Row> pageRow = pager.getPage(rs, pageable.getPageNumber() + 1);

        List<T> content = new ArrayList<>(pageRow.getElements().size());
        for (Row row : pageRow.getElements()) {
            T t = asEntity(row, this.getSingleRowMapper());
            if (t != null) content.add(t);
        }

        boolean hasNext = !pageRow.isLast(); // from OffsetPager
        return new SliceImpl<>(content, pageable, hasNext);
    }





    private <EntityT> EntityT asEntity(Row row, Function<Row, EntityT> mapper) {
        return row != null && (row.getColumnDefinitions().size() != 1 || !row.getColumnDefinitions().get(0).getName().equals(APPLIED)) ? mapper.apply(row) : null;
    }
}
