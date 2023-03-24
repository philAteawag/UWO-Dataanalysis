pkg load sqlite

% Connect to the database
conn = sqlite("Q:/Abteilungsprojekte/eng/SWWData/2015_fehraltorf/data_UWO_2019-01_2020-01.sqlite");

% Execute a query
results = sqlite_query (conn, "SELECT * FROM mytable");

% Display the results
disp(results);

% Close the connection
sqlite_close (conn);