import unittest

from core.data.etl import TransformData
import pyspark.sql.functions as F


class TestSpark(unittest.TestCase):


    def test_enrich_data(self):
        """
        Test if column total amount is positive
        """
        td = TransformData(data_path="test/src")

        # amout of records with total_amount is lower than 0 should be 0
        enriched_df = td.enrich_data().filter(F.col("total_amount") < 0).count()

        self.assertEqual(enriched_df, 0)

    def test_get_10_percent_data_based_on_distance_check_if_max_value_exists_in_output(self):
        """
        Test top 10%
        """
        td = TransformData(data_path="test/src")

        # Check if max value for field trip_distance exists in output of function
        get_10_percent = td.get_10_percent_data_based_on_distance()

        self.assertEqual(get_10_percent.select("trip_distance").first().trip_distance,
                         get_10_percent.select("trip_distance").rdd.max()[0])

    def test_get_10_percent_data_based_on_distance_check_if_amount_of_output_is_10_percent(self):
        """
        Test top 10%
        """
        td = TransformData(data_path="test/src")

        # Check if amount of output is in 10 percent

        all_data = td.enrich_data().count()

        top_10 = td.get_10_percent_data_based_on_distance().count()

        self.assertLess(all_data/top_10, 10)



if __name__ == '__main__':
    unittest.main()
