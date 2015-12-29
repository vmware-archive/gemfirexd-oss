
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('seats', 'COUNTRY', '/export/gcm/where/schemas/seatsdata/country.csv', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('seats', 'AIRPORT', '/export/gcm/where/schemas/seatsdata/airport.csv', '|', null, '8859_1', 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('seats', 'AIRPORT_DISTANCE', '/export/gcm/where/schemas/seatsdata/airport_distance.csv', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('seats', 'AIRLINE', '/export/gcm/where/schemas/seatsdata/airline.csv', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('seats', 'CUSTOMER', '/export/gcm/where/schemas/seatsdata/customer.csv', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('seats', 'FREQUENT_FLYER', '/export/gcm/where/schemas/seatsdata/frequent_flyer.csv', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('seats', 'FLIGHT', '/export/gcm/where/schemas/seatsdata/flight.csv', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('seats', 'RESERVATION', '/export/gcm/where/schemas/seatsdata/reservation.csv', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('seats', 'CONFIG_PROFILE', '/export/gcm/where/schemas/seatsdata/config_profile.csv', '|', null, null, 0, 0, 6, 0, null, null);
CALL SYSCS_UTIL.IMPORT_TABLE_EX ('seats', 'CONFIG_HISTOGRAMS', '/export/gcm/where/schemas/seatsdata/config_histograms.csv', '|', null, null, 0, 0, 6, 0, null, null);
