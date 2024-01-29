
class GetData:

    def __init__(self, url_path, file_pattern, format, dates):

        self.url_path = url_path
        self.file_pattern = file_pattern
        self.format = format

        self.dates = dates


    def build_directory(self):

        import os

        return os.path.join('core', 'temp', 'data')

    def build_url(self, date):

        print(f"{self.url_path}/{self.file_pattern}_{date}.{self.format}".replace('"',''))

        return f"{self.url_path}/{self.file_pattern}_{date}.{self.format}".replace('"','')

    def save_date(self, r, date):

        file_path = self.build_directory()

        open(f'{file_path}/{self.file_pattern}_{date}.{self.format}'.replace('"',''), 'wb').write(r.content)


    def get_files_by_date_range(self):

        import requests
        import time

        print("-------------------------------")
        print("\n------ingest data----------\n")
        print("-------------------------------")

        for date in self.dates:

            print(f"\n handling date: {date} \n")

            # to avoid blocking by multiple requests
            time.sleep(2)

            url = self.build_url(date)

            try:

                if "AccessDenied" in requests.get(url, allow_redirects=True).text:
                    print("------data doesn't exist on website----------")

                else:

                    r = requests.get(url, allow_redirects=True)

                    self.save_date(r, date)

            except:
                print("url doesn't exist")