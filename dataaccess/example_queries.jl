using DataFrames
using SQLite


function example_query_1(db_file::String)
    end_time = "2021-09-05"
    start_time = "2021-09-01"
    location = "11e_russikerstr"
    example_query = """
    SELECT
        signal.timestamp,
        value,
        unit,
        variable.name,
        source_type.name,
        source.name
    FROM signal
        INNER JOIN site ON signal.site_id = site.site_id
        INNER JOIN variable ON signal.variable_id = variable.variable_id
        INNER JOIN source ON signal.source_id = source.source_id
        INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    WHERE site.name = '$location'
        AND signal.timestamp >= '$start_time'
        AND signal.timestamp <= '$end_time';
    """
    db = SQLite.DB(db_file)
    result = SQLite.execute(db, example_query)SQLite.execute
    close(db)
    return DataFrame(result)
end

function example_query_2(db_file::String)
    variable = "water_temperature"
    example_query = """
    WITH variable_ids as (
        SELECT variable_id FROM variable WHERE variable.name = '$variable'
    ), source_ids as (
        SELECT DISTINCT source_id FROM signal
        WHERE signal.variable_id IN (
            SELECT variable_id FROM variable_ids
        )
    )
    SELECT source.name from source
    WHERE source.source_id IN (
        SELECT source_id from source_ids
    )
    """
    db = SQLite.DB(db_file)
    result = SQLite.execute(db, example_query)
    close(db)
    return DataFrame(result)
end

using DataFrames, SQLite

function example_query_3(db_file::String)
    type = "DS18B20"
    example_query = """
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
    result = SQLite.execute(db, example_query)
    close(db)
    return DataFrame(result)
end

function example_query_4(db_file::String)
    example_query = """
    WITH count_table AS (
    SELECT 
        count(variable_id), 
        variable_id, 
        source_id, 
        date_trunc('week', timestamp)
    FROM signal
    GROUP BY 
        date_trunc('week', timestamp), 
        variable_id, 
        source_id
    )
    SELECT 
        count_table.count AS value_count, 
        variable.name AS variable_name, 
        source.name AS source_name,
        count_table.date_trunc AS date_trunc
    FROM count_table
    INNER JOIN variable ON variable.variable_id = count_table.variable_id
    INNER JOIN source ON source.source_id = count_table.source_id
    order by date_trunc desc;
    """
    db = SQLite.DB(db_file)
    result = SQLite.execute(db, example_query)
    close(db)
    return DataFrame(result)
end

function example_query_5(db_file::String, cl::String)
    content_a1 = DataFrame(CSV.File(cl, delim=';'))
    filter_a1 = content_a1[content_a1.A1 .== 1,:]
    source_names_a1 = collect(filter_a1.source)
    example_query = """
    SELECT
    signal.timestamp,
    value,
    unit,
    variable.name,
    source_type.name,
    source.name
    FROM signal
    INNER JOIN site ON signal.site_id = site.site_id
    INNER JOIN variable ON signal.variable_id = variable.variable_id
    INNER JOIN source ON signal.source_id = source.source_id
    INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    WHERE source.name IN ($(fill("?, ", length(source_names_a1)-1)...),?)
    """

    db = SQLite.DB(db_file)
    result = SQLite.execute(db, example_query)
    close(db)
    return DataFrame(result)
end

function example_query_6(db_file::String, cl::String)
    content_a2 = DataFrame(CSV.File(cl, delim=';'))
    filter_a2 = content_a4[content_a2.A2 .== 1,:]
    source_names_a2 = collect(filter_a2.source)
    example_query = """
    SELECT
    signal.timestamp,
    value,
    unit,
    variable.name,
    source_type.name,
    source.name
    FROM signal
    INNER JOIN site ON signal.site_id = site.site_id
    INNER JOIN variable ON signal.variable_id = variable.variable_id
    INNER JOIN source ON signal.source_id = source.source_id
    INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    WHERE source.name IN ($(fill("?, ", length(source_names_a2)-1)...),?)
    """
    db = SQLite.DB(db_file)
    result = SQLite.execute(db, example_query)
    close(db)
    return DataFrame(result)
end

function example_query_7(db_file::String, cl::String)
    content_a3 = DataFrame(CSV.File(cl, delim=';'))
    filter_a3 = content_a4[content_a3.A3 .== 1,:]
    source_names_a3 = collect(filter_a3.source)
    example_query = """
    SELECT
    signal.timestamp,
    value,
    unit,
    variable.name,
    source_type.name,
    source.name
    FROM signal
    INNER JOIN site ON signal.site_id = site.site_id
    INNER JOIN variable ON signal.variable_id = variable.variable_id
    INNER JOIN source ON signal.source_id = source.source_id
    INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    WHERE source.name IN ($(fill("?, ", length(source_names_a3)-1)...),?)
    """
    db = SQLite.DB(db_file)
    result = SQLite.execute(db, example_query)
    close(db)
    return DataFrame(result)
end

function example_query_8(db_file::String, cl::String)
    content_a4 = DataFrame(CSV.File(cl, delim=';'))
    filter_a4 = content_a4[content_a4.A4 .== 1,:]
    source_names_a4 = collect(filter_a4.source)
    example_query = """
    SELECT
    signal.timestamp,
    value,
    unit,
    variable.name,
    source_type.name,
    source.name
    FROM signal
    INNER JOIN site ON signal.site_id = site.site_id
    INNER JOIN variable ON signal.variable_id = variable.variable_id
    INNER JOIN source ON signal.source_id = source.source_id
    INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    WHERE source.name IN ($(fill("?, ", length(source_names_a4)-1)...),?)
    """
    db = SQLite.DB(db_file)
    result = SQLite.execute(db, example_query)
    close(db)
    return DataFrame(result)
end

function main(sourcedirectory, filename, contentlist)
    db = joinpath(sourcedirectory, filename)
    cl = joinpath(sourcedirectory, contentlist)

    db = SQLite.DB(db)
    result = DataFrame(SQLite.execute(db, "SELECT * FROM site LIMIT 5;"))
    close(db)
    println(result)

    # println(example_query_1(db))
    # println(example_query_2(db))
    # println(example_query_3(db))
    # println(example_query_4(db))
    # println(example_query_5(db, cl))
    # println(example_query_6(db, cl))
    # println(example_query_7(db, cl))
    # println(example_query_8(db, cl))

end

sourcedirectory ="Q:/Abteilungsprojekte/eng/SWWData/2015_fehraltorf/uwo_data_slices"
filename="data_UWO_2020-01_2021-01.sqlite"
contentlist="package_information.csv"
main(sourcedirectory, filename, contentlist)
