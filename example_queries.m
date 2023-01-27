pkg load sqlite

% Connect to the database
conn = sqlite("mydatabase.db");

% Execute a query
results = sqlite_query (conn, "SELECT * FROM mytable");

% Display the results
disp(results);

% Close the connection
sqlite_close (conn);