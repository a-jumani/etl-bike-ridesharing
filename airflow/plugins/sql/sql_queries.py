class SqlInitTables:

    drop_staging_tables = """
        DROP TABLE IF EXISTS public.staging_bike_rides;
        DROP TABLE IF EXISTS public.staging_holiday;
        DROP TABLE IF EXISTS public.staging_temperature;
        DROP TABLE IF EXISTS public.staging_humidity;
        DROP TABLE IF EXISTS public.staging_weather_desc;
    """

    clear_staging_tables = """
        DELETE FROM public.staging_bike_rides;
        DELETE FROM public.staging_holiday;
        DELETE FROM public.staging_temperature;
        DELETE FROM public.staging_humidity;
        DELETE FROM public.staging_weather_desc;
    """

    drop_fact_n_dims_tables = """
        DROP TABLE IF EXISTS public.fact_bike_rides;
        DROP TABLE IF EXISTS public.dim_time;
        DROP TABLE IF EXISTS public.dim_station;
        DROP TABLE IF EXISTS public.dim_weather_desc;
        DROP TABLE IF EXISTS public.dim_holiday;
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

        CREATE TABLE IF NOT EXISTS public.staging_holiday (
            day     VARCHAR (10) NOT NULL,
            date    VARCHAR (15) NOT NULL,
            holiday VARCHAR (50) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS public.staging_temperature (
            datetime TIMESTAMP NOT NULL,
            value    NUMERIC (7, 3)
        );

        CREATE TABLE IF NOT EXISTS public.staging_humidity (
            datetime TIMESTAMP NOT NULL,
            value    NUMERIC (5, 1)
        );

        CREATE TABLE IF NOT EXISTS public.staging_weather_desc (
            datetime TIMESTAMP NOT NULL,
            value    VARCHAR (50)
        );
    """

    create_fact_n_dims_tables = """
        CREATE TABLE IF NOT EXISTS public.fact_bike_rides (
            id                 INTEGER IDENTITY (1, 1) PRIMARY KEY,
            customer_id        INTEGER,
            gender             SMALLINT NOT NULL DEFAULT 0,
            pickup_time        TIMESTAMP NOT NULL,
            dropoff_time       TIMESTAMP NOT NULL,
            pickup_station_id  INTEGER NOT NULL,
            dropoff_station_id INTEGER NOT NULL,
            trip_duration      INTEGER NOT NULL,
            weather_desc_id    SMALLINT NOT NULL,
            temperature        NUMERIC (7, 3) NOT NULL,
            humidity           NUMERIC (5, 1) NOT NULL
        )
        DISTKEY (pickup_time)
        SORTKEY (pickup_time);

        CREATE TABLE IF NOT EXISTS public.dim_time (
            time    TIMESTAMP PRIMARY KEY,
            hour    SMALLINT NOT NULL,
            day     SMALLINT NOT NULL,
            week    SMALLINT NOT NULL,
            month   SMALLINT NOT NULL,
            year    SMALLINT NOT NULL,
            weekday SMALLINT NOT NULL
        )
        DISTKEY (time)
        SORTKEY (time);

        CREATE TABLE IF NOT EXISTS public.dim_station (
            id        INTEGER IDENTITY (1, 1) PRIMARY KEY,
            longitude NUMERIC (13, 10) NOT NULL,
            latitude  NUMERIC (13, 10) NOT NULL
        )
        DISTSTYLE ALL
        SORTKEY (id);

        CREATE TABLE IF NOT EXISTS public.dim_weather_desc (
            id   INTEGER IDENTITY (1, 1) PRIMARY KEY,
            desp VARCHAR (50) NOT NULL
        )
        DISTSTYLE ALL
        SORTKEY (id);

        CREATE TABLE IF NOT EXISTS public.dim_holiday (
            date DATE PRIMARY KEY,
            desp VARCHAR (50)
        )
        DISTSTYLE ALL
        SORTKEY (date);
    """
