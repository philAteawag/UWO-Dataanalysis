using sdts

# generate info to be able to connect to database
connection_string = make_connection_string(
    host = "eaw-sdwh3.eawag.wroot.emp-eaw.ch",
    user = "datapool",
    port = 5432,
    database = "datapool",
    password = "corona666"
)

# set source name
source_name = "bt_dl933_11e_russikerstr"
source_name = "bt_dl916_inflow_ara"
source_name = "bt_dl923_166_luppmenweg"
# source_name = "bt_dl937_58sbw_undermulistr"
source_name = "bt_dl925_581a_wildbach"
source_name = "bt_dl934_47a_zurcherstr"
# source_name = "bt_dl946_555_mesikerstr"
# source_name = "bt_dl931_vs22_kempttalstr"
# source_name = "bt_dl924_137_schutzengasse"
# source_name = "bt_dl930_40d_imberg"

# list SD card data (helper function to select sd data)
datamap = get_temp_sd_files("Q:/Abteilungsprojekte/eng/SWWData/2015_fehraltorf/3_data/_datapool_preparation/metadata")

# select sd card file
sd_file = datamap[source_name][1]
sd_file = "Q:/Abteilungsprojekte/eng/SWWData/2015_fehraltorf/3_data/_datapool_preparation/metadata/bt_dl923_166_luppmenweg/rawdata/220303/00923_210421_0830.csv"

# read file using the sensor id or ids
data_sd_raw = read_sd_data(
    sd_file,
    idsWater=[20838, 14506],
    idsAir=[22937, 48298]
)

# find threshold to identify restarts of sensor loggers
# (peaks in plot)
# plot(abs.(diff(data_sd_raw[:,1])))

# identify sections without logger restart
no_restart_sections = divide_sd_into_sections(data_sd_raw, threshold=1e5)

if isempty(no_restart_sections)
    throw(DomainError(no_restart_sections, "this array is empty"))
end

# retrieve data from datapool
start_time = "2019-01-01 00:00:00"
end_time = format(now(), "Y-m-d H:M:S")
data = query_source(
    connection_string,
    source_name,
    start_time,
    end_time
)

# transform table to needed format
data_pool = transform_pool_data(data)

for i in 1:length(no_restart_sections)
    # select a single section without logger restart
    no_restart_sections_id = i
    st, en = no_restart_sections[no_restart_sections_id]
    data_sd = data_sd_raw[st:en,:]

    # plot sd card and datapool data in direct comparison
    # plot_data(data_sd, data_pool)

    # identify sections without gaps in the datapool data
    min_good_length = 80 # values without gaps
    good_sections = get_gapless(data_pool[:,1], min_good_length)

    # make sure the found good section do not contain any sections of constant values
    good_sections = filter_good_sections(data_pool, good_sections)
    println("Found sections: ", length(good_sections))

    # identify sections section that match between sd card data to datapool data
    @time fitted = find_matching_sections(data_sd, data_pool, good_sections)

    # calculate the timestamps and print the start timestamps calculated for matches
    timestamps = print_sd_start_timestamps(data_sd, data_pool, fitted)

    # plot the a matched section to check the match
    # pp = plot_matched_section(1,data_sd, data_pool, fitted, timestamps)

    # analysing drift within each matched section
    timestamps_short, drift_rates_short = analyse_drift_in_sections(data_sd, data_pool, fitted)
    if isempty(drift_rates_short)
        continue
    end
    med_drift_short = median(drift_rates_short)
    println("median: " ,med_drift_short)

    # analysing drift between matched sections
    drift_rates_long = analyse_drift_between_sections(data_sd, data_pool, fitted)
    med_drift_long = median(drift_rates_long)
    println("median: ", med_drift_long)
    println("median drift in seconds per day: ", med_drift_long*60*24)

    # correct drift using the more reliable drift analysis between sections
    corrected_data_sd = correct_drift(data_sd, med_drift_long)

    # get an overview of drift analysis
    println("---- before Correction ----")
    uncorrected_timestamps = print_sd_start_timestamps(data_sd, data_pool, fitted)
    println("\n---- after Correction ----")
    corrected_timestamps = print_sd_start_timestamps(corrected_data_sd, data_pool, fitted)

    # calculate the mean timestamp between all matched sections
    final_timestamp = mean(corrected_timestamps, dims=2)

    # check the offsets of each matched section regarding the mean timestamp
    difference_support_points = check_support_points(final_timestamp, data_pool, fitted)

    # save the drift analysis information with plots
    filepath = "C:/Users/dischand/switchdrive/UWO/Arbeiten und Artikel/UWO_Data_paper/QoS-Analyse/sd_matched_data/$source_name" * "_" * "$no_restart_sections_id" * ".pdf"
    save_result_in_pdf(filepath, drift_rates_short, drift_rates_long, difference_support_points, fitted, source_name, final_timestamp)

    # save the final timestamp to the sd card data
    filepath = "C:/Users/dischand/switchdrive/UWO/Arbeiten und Artikel/UWO_Data_paper/QoS-Analyse/sd_matched_data/$source_name"*"_"*"$no_restart_sections_id"*".csv"
    save_data_with_timestamp(filepath, sd_file, no_restart_sections, no_restart_sections_id, final_timestamp)
end