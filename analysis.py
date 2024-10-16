sql =''' SELECT * FROM sentiment_tbl
        ORDER BY num_appeared DESC
        LIMIT 1;
    '''