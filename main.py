from pyspark.sql import SparkSession

from src.utils import read_yaml
from src.us_vehicle_accident_analytics import USVehicleAccidentAnalytics

if __name__ == "__main__":
    # Initialize spark session
    spark = SparkSession.builder.appName("USVehicleAccidentAnalytics").getOrCreate()

    config_file_name = "config.yaml"
    spark.sparkContext.setLogLevel("ERROR")

    config = read_yaml(config_file_name)
    output_file_paths = config.get("OUTPUT_PATH")
    file_format = config.get("FILE_FORMAT")

    usvaa = USVehicleAccidentAnalytics(spark, config)

    # 1. Find the number of crashes (accidents) in which number of males killed are greater than 2?
    print(
        "Analytics 1 Result:",
        usvaa.count_male_crashes_greater_than_2(output_file_paths.get(1), file_format.get("Output")),
    )

    # 2. How many two wheelers are booked for crashes?
    print(
        "Analytics 2 Result:",
        usvaa.count_two_wheeler_crashes(
            output_file_paths.get(2), file_format.get("Output")
        ),
    )

    # 3. Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died
    # and Airbags did not deploy
    print(
        "Analytics 3 Result:",
        usvaa.top_5_vehicle_makes_for_fatal_crashes_without_airbags(
            output_file_paths.get(3), file_format.get("Output")
        ),
    )

    # 4. Determine the number of Vehicles with a driver having valid licences involved in hit and run?
    print(
        "Analytics 4 Result:",
        usvaa.count_hit_and_run_with_valid_licenses(
            output_file_paths.get(4), file_format.get("Output")
        ),
    )

    # 5. Which state has the highest number of accidents in which females are not involved?
    print(
        "Analytics 5 Result:",
        usvaa.find_state_with_no_female_accident(
            output_file_paths.get(5), file_format.get("Output")
        ),
    )

    # 6. Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    print(
        "Analytics 6 Result:",
        usvaa.get_top_vehicle_id_contributing_to_injuries(
            output_file_paths.get(6), file_format.get("Output")
        ),
    )

    # 7. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    print("Analytics 7 Result:")
    usvaa.display_top_ethnic_ug_by_body_style(
        output_file_paths.get(7), file_format.get("Output")
    ).show(truncate=False)

    # 8. Among the crashed cars, what are the Top 5 Zip Codes with the highest number of crashes with alcohol as the
    # contributing factor to a crash (Use Driver Zip Code)
    print(
        "Analytics 8 Result:",
        usvaa.find_top_zip_codes_alcohol_crashes(
            output_file_paths.get(8), file_format.get("Output")
        ),
    )

    # 9. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above
    # 4 and car avails Insurance
    print(
        "Analytics 9 Result:",
        usvaa.count_crash_ids_with_no_damage(
            output_file_paths.get(9), file_format.get("Output")
        ),
    )

    # 10. Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed
    # Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
    # offenses (to be deduced from the data)
    print(
        "Analytics 10 Result:",
        usvaa.get_top_5_vehicle_makes_speeding_offenses(
            output_file_paths.get(10), file_format.get("Output")
        ),
    )

    spark.stop()
