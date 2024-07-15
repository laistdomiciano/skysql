from sqlalchemy import create_engine, text

QUERY_FLIGHT_BY_ID = """
SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY 
FROM flights 
JOIN airlines ON flights.airline = airlines.id 
WHERE flights.ID = :id
"""

QUERY_FLIGHTS_BY_DATE = """
SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY 
FROM flights 
JOIN airlines ON flights.airline = airlines.id 
WHERE date(flights.departure_time) = date(:day || '-' || :month || '-' || :year)
"""

QUERY_DELAYED_FLIGHTS_BY_AIRLINE = """
SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY 
FROM flights 
JOIN airlines ON flights.airline = airlines.id 
WHERE airlines.airline = :airline AND flights.DEPARTURE_DELAY > 0
"""

QUERY_DELAYED_FLIGHTS_BY_AIRPORT = """
SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY 
FROM flights 
JOIN airlines ON flights.airline = airlines.id 
WHERE flights.origin_airport = :airport AND flights.DEPARTURE_DELAY > 0
"""

class FlightData:
    """
    The FlightData class is a Data Access Layer (DAL) object that provides an
    interface to the flight data in the SQLITE database. When the object is created,
    the class forms a connection to the SQLite database file, which remains active
    until the object is destroyed.
    """
    def __init__(self, db_uri):
        """
        Initialize a new engine using the given database URI
        """
        self._engine = create_engine(db_uri)

    def _execute_query(self, query, params):
        """
        Execute an SQL query with the params provided in a dictionary,
        and returns a list of records (dictionary-like objects).
        If an exception was raised, print the error, and return an empty list.
        """
        try:
            with self._engine.connect() as connection:
                result = connection.execute(text(query), params)
                return [row for row in result]
        except Exception as e:
            print("Error executing query:", e)
            return []

    def get_flight_by_id(self, flight_id):
        """
        Searches for flight details using flight ID.
        If the flight was found, returns a list with a single record.
        """
        params = {'id': flight_id}
        return self._execute_query(QUERY_FLIGHT_BY_ID, params)

    def get_flights_by_date(self, day, month, year):
        """
        Searches for flight details using a specific date.
        """
        params = {'day': day, 'month': month, 'year': year}
        print(params)
        return self._execute_query(QUERY_FLIGHTS_BY_DATE, params)

    def get_delayed_flights_by_airline(self, airline):
        """
        Searches for delayed flights by airline name.
        """
        params = {'airline': airline}
        return self._execute_query(QUERY_DELAYED_FLIGHTS_BY_AIRLINE, params)

    def get_delayed_flights_by_airport(self, airport):
        """
        Searches for delayed flights by origin airport code.
        """
        params = {'airport': airport}
        return self._execute_query(QUERY_DELAYED_FLIGHTS_BY_AIRPORT, params)

    def __del__(self):
        """
        Closes the connection to the database when the object is about to be destroyed.
        """
        self._engine.dispose()