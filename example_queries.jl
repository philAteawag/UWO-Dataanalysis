using SQLite
using DataFrames

using SQLite
using ArgParse

function open_sqlite(db_file)
    conn = SQLite.DB(db_file)
    try
        yield(conn)
    catch e
        rollback(conn)
        throw(e)
    finally
        close(conn)
    end
end

function connect(db_file)
    return SQLite.DB(db_file)
end

function _query_plain(conn, sql_query)
    stmt = prepare(conn, sql_query)
    res = execute(stmt)
    close(stmt)
    return res
end

function _query_df(conn, sql_query)
    return DataFrame(query(conn, sql_query))
end

function query(db_file, sql_query, return_dataframe=true)
    conn = open_sqlite(db_file)
    if return_dataframe
        return _query_df(conn, sql_query)
    else
        return _query_plain(conn, sql_query)
    end
end

function example_query_1(db_file::String)
    end_time = DateTime.now()
    start_time = end_time - Day(30)

    location = "11e_russikerstr"

    example_query = """
    SELECT
        signal.timestamp,
        value,
        unit,
        parameter.name,
        source_type.name,
        source.name
    FROM signal
        INNER JOIN site ON signal.site_id = site.site_id
        INNER JOIN parameter ON signal.parameter_id = parameter.parameter_id
        INNER JOIN source ON signal.source_id = source.source_id
        INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    WHERE site.name = '$location'
        AND signal.timestamp >= '$start_time'
        AND signal.timestamp <= '$end_time';
    """

    return query(db_file, example_query)
end

function example_query_2(db_file::String)
    parameter = "water_temperature"

    example_query = """
    WITH parameter_ids as (
        SELECT parameter_id FROM parameter WHERE parameter.name = '$parameter'
    ), source_ids as (
        SELECT DISTINCT source_id FROM signal
        WHERE signal.parameter_id IN (
            SELECT parameter_id FROM parameter_ids
        )
    )
    SELECT source.name from source
    WHERE source.source_id IN (
        SELECT source_id from source_ids
    )
    """

    return query(db_file, example_query)
end

using DataFrames, SQLite

function example_query_3(db_file::String)
    type = "DS18B20"
    query = """
    SELECT
        source.name,
        MAX(signal.timestamp)
    FROM signal
        INNER JOIN source ON signal.source_id = source.source_id
        INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    WHERE source_type.name = '$(type)'
    GROUP BY source.name
    ORDER BY MAX(signal.timestamp) ASC;
    """

    db = SQLite.DB(db_file)
    result = SQLite.query(db, query)
    close(db)
    return DataFrame(result)
end

function example_query_4(db_file::String)
    query = """
    WITH count_table AS (
    SELECT 
        count(parameter_id), 
        parameter_id, 
        source_id, 
        date_trunc('week', timestamp)
    FROM signal
    GROUP BY 
        date_trunc('week', timestamp), 
        parameter_id, 
        source_id
    )
    SELECT 
        count_table.count AS value_count, 
        parameter.name AS parameter_name, 
        source.name AS source_name,
        count_table.date_trunc AS date_trunc
    FROM count_table
    INNER JOIN parameter ON parameter.parameter_id = count_table.parameter_id
    INNER JOIN source ON source.source_id = count_table.source_id
    order by date_trunc desc;
    """

    db = SQLite.DB(db_file)
    result = SQLite.query(db, query)
    close(db)
    return DataFrame(result)
end

function example_query_5(db_file::String)
    query = """
    """
    db = SQLite.DB(db_file)
    result = SQLite.query(db, query)
    close(db)
    return DataFrame(result)
end

function example_query_6(db_file::String)
    query = """
    """
    db = SQLite.DB(db_file)
    result = SQLite.query(db, query)
    close(db)
    return DataFrame(result)
end

function example_query_7(db_file::String)
    query = """
    """
    db = SQLite.DB(db_file)
    result = SQLite.query(db, query)
    close(db)
    return DataFrame(result)
end

function example_query_8(db_file::String)
    query = """
    """
    db = SQLite.DB(db_file)
    result = SQLite.query(db, query)
    close(db)
    return DataFrame(result)
end

function main(args::ArgParse.Namespace)

    path_to_db = pathlib.Path(args.sourcedirectory)

    db = path_to_db / "dp_copy.sqlite"

    println(query(db, "SELECT name FROM source_type"))

    println(example_query_1(db_file=db))
    println(example_query_2(db_file=db))
    println(example_query_3(db_file=db))
    println(example_query_4(db_file=db))
    println(example_query_5(db_file=db))
    println(example_query_6(db_file=db))
    println(example_query_7(db_file=db))
    println(example_query_8(db_file=db))

end

parser = ArgParse.ArgumentParser()
ArgParse.add_argument!(parser, ["-sd", "--sourcedirectory"], default="/path/to/db_file")

args = ArgParse.parse_args(parser)

main(args)
