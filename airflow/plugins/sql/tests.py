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
