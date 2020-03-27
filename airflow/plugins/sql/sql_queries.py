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


class SqlQueries:

    load_dim_time = """
        SELECT ts, EXTRACT(hour FROM ts), EXTRACT(day FROM ts),
            EXTRACT(week FROM ts), EXTRACT(month FROM ts),
            EXTRACT(year FROM ts), EXTRACT(dayofweek FROM ts)
        FROM (SELECT pickup_datetime AS ts FROM public.staging_bike_rides
              UNION
              SELECT dropoff_datetime AS ts FROM public.staging_bike_rides)
    """

    load_dim_weather_desc = """
        SELECT DISTINCT value as desp
        FROM public.staging_weather_desc
        WHERE value IS NOT NULL
    """

    load_dim_station = """
        (SELECT pickup_longitude AS longitude, pickup_latitude AS latitude
        FROM public.staging_bike_rides)
        UNION
        (SELECT dropoff_longitude AS longitude, dropoff_latitude AS latitude
        FROM public.staging_bike_rides)
    """

    load_dim_holiday = """
        SELECT TO_DATE(CONCAT(date, ' 2016'), 'Month DD YYYY'), holiday
        FROM public.staging_holiday
    """

    load_fact_bike_rides = """
        SELECT r.id, r.gender, r.pickup_datetime, r.dropoff_datetime, sp.id,
            sd.id, r.trip_duration, w.id, t.value, h.value
        FROM public.staging_bike_rides r
            JOIN public.dim_station sp
                ON r.pickup_longitude=sp.longitude AND
                    r.pickup_latitude=sp.latitude
            JOIN public.dim_station sd
                ON r.dropoff_longitude=sd.longitude AND
                    r.dropoff_latitude=sd.latitude
            LEFT JOIN public.staging_temperature t
                ON DATE_TRUNC('hour', r.pickup_datetime)=t.datetime
            LEFT JOIN public.staging_humidity h
                ON DATE_TRUNC('hour', r.pickup_datetime)=h.datetime
            LEFT JOIN (SELECT sw.datetime AS datetime, dw.id AS id
                       FROM public.staging_weather_desc sw
                           JOIN public.dim_weather_desc dw
                               ON sw.value=dw.desp) w
                ON DATE_TRUNC('hour', r.pickup_datetime)=w.datetime
    """
