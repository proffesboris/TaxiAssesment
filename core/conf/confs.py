import configparser

def take_configuration():

    conf_data = {}

    config = configparser.ConfigParser()

    import os

    config.read(os.path.join('core', 'conf', 'scrapping_info.ini'))

    (
     conf_data["url_root_path"],
     conf_data["file"],
     conf_data["format"]
    ) = (
        config.get('yellow_taxi', 'url_root'),
        config.get('yellow_taxi', 'file'),
        config.get('yellow_taxi', 'file_format'))

    return conf_data


