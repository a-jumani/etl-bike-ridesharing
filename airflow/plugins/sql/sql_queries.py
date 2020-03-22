class SqlInitTables:

    drop_staging_tables = """
        DROP TABLE IF EXISTS public.staging_bike_rides;
    """

    clear_staging_tables = """
        DELETE FROM public.staging_bike_rides;
    """

    create_staging_tables = """
        CREATE TABLE IF NOT EXISTS public.staging_bike_rides (
            id                INTEGER NOT NULL,
            gender            SMALLINT NOT NULL,
            pickup_datetime   TIMESTAMP NOT NULL,
            dropoff_datetime  TIMESTAMP NOT NULL,
            pickup_longitude  NUMERIC (13, 10) NOT NULL,
            pickup_latitude   NUMERIC (13, 10) NOT NULL,
            dropoff_longitude NUMERIC (13, 10) NOT NULL,
            dropoff_latitude  NUMERIC (13, 10) NOT NULL,
            trip_duration     INTEGER NOT NULL
        );
    """
