
from core.data.etl import TransformData
from core.web_scraping.ingest import GetData

from core.conf.confs import take_configuration
from core.utils.tools import handle_args, build_list_of_dates


if __name__ == '__main__':

    st_dt, end_dt = handle_args()

    cf = take_configuration()

    dates = build_list_of_dates(st=st_dt,en=end_dt)

    ingest_data = GetData(
        url_path=cf["url_root_path"],
        file_pattern=cf["file"],
        format=cf["format"],
        dates=dates)


    ingest_data.get_files_by_date_range()

    data = TransformData()

    data.save_results_on_disk()



