from auth_service import get_authenticated_service
from channels import ChannelsHandler
from reports import ReportsHandler
from videos import VideosHandler
from bigquery import run_job
import pandas as pd
import datetime
import logging
import json
import os

# Logging config
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s :: %(levelname)s :: %(message)s"
)

# Read config file
with open("config.json", "r") as jsonfile:
    data = json.load(jsonfile)
    logging.info("Config file read successfully!")

# Global Constants
CONTENT_OWNER = data["CONTENT_OWNER"]
CLIENT_SECRETS_FILE = data["AUTH"]["CLIENT_SECRETS_FILE"]
SCOPES = data["AUTH"]["SCOPES"]

# Reporting Constants
REPORTING_API_SERVICE_NAME = data["AUTH"]["REPORTING_API_SERVICE_NAME"]
REPORTING_API_VERSION = data["AUTH"]["REPORTING_API_VERSION"]
REPORT_FOLDER = data["REPORTING"]["FOLDER"]
REPORT_SCHEMA = data["TABLES"]["reports"]["schema"]
REPORT_NAME = data["REPORTING"]["NAME"]
START_DATE = data["REPORTING"]["START_DATE"]
LAST_DATE = data["REPORTING"]["LAST_DATE"]
JOB_ID = data["REPORTING"]["JOB_ID"]

# Data Constants
DATA_API_SERVICE_NAME = data["AUTH"]["DATA_API_SERVICE_NAME"]
DATA_API_VERSION = data["AUTH"]["DATA_API_VERSION"]


if __name__ == "__main__":
    if(LAST_DATE <= datetime.date.today().strftime("%Y-%m-%dT%H:%M:%SZ")):
        youtube_reporting = get_authenticated_service(
            CLIENT_SECRETS_FILE,
            SCOPES,
            REPORTING_API_SERVICE_NAME,
            REPORTING_API_VERSION,
        )

        reports_handler = ReportsHandler(
            youtube_reporting,
            CONTENT_OWNER,
            JOB_ID,
            LAST_DATE,
            START_DATE,
            REPORT_SCHEMA,
            REPORT_NAME,
            REPORT_FOLDER,
        )
        
        # Run Reports
        reports_handler.run_reports()
        for report in os.scandir(REPORT_FOLDER):
            if('processed' in report.name):
                df = pd.read_csv(report)
                df['date'] = pd.to_datetime(df["date"])
                run_job(df, data["TABLES"]["reports"])

    if os.listdir(REPORT_FOLDER):
        youtube_data = get_authenticated_service(
            CLIENT_SECRETS_FILE, 
            SCOPES, 
            DATA_API_SERVICE_NAME, 
            DATA_API_VERSION
        )

        channels_handler = ChannelsHandler(
            youtube_data=youtube_data, content_owner_id=CONTENT_OWNER
        )

        videos_handler = VideosHandler(
            youtube_data=youtube_data, content_owner_id=CONTENT_OWNER, report_folder=REPORT_FOLDER
        )

        # Run Channels
        channels = pd.DataFrame(channels_handler.get_channels())
        run_job(channels, data["TABLES"]["channels"])

        # Run Videos
        videos = pd.DataFrame(videos_handler.get_videos())
        run_job(videos, data["TABLES"]["videos"])

        # Run Video Categories
        categories = pd.DataFrame(videos_handler.get_categories(videos=videos))
        run_job(categories, data["TABLES"]["video_categories"])
    else:
        logging.log('No reports to process')