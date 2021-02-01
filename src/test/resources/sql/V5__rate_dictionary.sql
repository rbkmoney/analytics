CREATE DICTIONARY rate_dictionary (
    id 				            UInt64,
    event_id 		            UInt64,
    event_time		            String,
    source_id		            String,
    lower_bound_inclusive		String,
    upper_bound_exclusive		String,
    source_symbolic_code		String,
    source_exponent		        UInt32,
    destination_symbolic_code   String,
    exchange_rate_rational_p    UInt64,
    exchange_rate_rational_q    UInt64
)
PRIMARY KEY id
SOURCE(ODBC(connection_string 'DSN=myconnection;UID=user;PWD=password;HOST=host;PORT=5432;DATABASE=analytics' table 'analytics.rate'))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360)
