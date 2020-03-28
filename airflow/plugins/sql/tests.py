class SqlStagingTestsCheck:

    check_holidays = "SELECT COUNT(*) FROM public.staging_holiday"

    check_temperature = "SELECT COUNT(*) FROM public.staging_temperature"

    check_humidity = "SELECT COUNT(*) FROM public.staging_humidity"

    check_weather_desc = "SELECT COUNT(*) FROM public.staging_weather_desc"

    check_value_bike_rides = {
        "query": "SELECT COUNT(*) FROM public.staging_bike_rides",
        "value": "4500000",
        "tolerance": None
    }


class SqlFactsNDimTestsCheck:

    check_weather_desc = "SELECT COUNT(*) FROM public.dim_weather_desc"

    check_value_time = {
        "query": "SELECT MAX(month) FROM public.dim_time",
        "value": "7",
        "tolerance": 0
    }

    check_value_holidays = {
        "query": "SELECT (SELECT COUNT(*) FROM public.staging_holiday) - \
            (SELECT COUNT(*) FROM public.dim_holiday)",
        "value": 0,
        "tolerance": None
    }

    check_value_station = {
        "query": "SELECT COUNT(*) FROM public.dim_station",
        "value": 500,
        "tolerance": 50
    }

    check_value_bike_rides = {
        "query": "SELECT (SELECT COUNT(*) FROM public.staging_bike_rides) - \
            (SELECT COUNT(*) FROM public.fact_bike_rides)",
        "value": "0",
        "tolerance": None
    }
