###
#
# This script retrieves YouTube Reporting API reports. Use cases:
# 1. If you specify a report URL, the script downloads that report.
# 2. Otherwise, if you specify a job ID, the script retrieves a list of
#    available reports for that job and prompts you to select a report.
#    Then it retrieves that report as in case 1.
# 3. Otherwise, the list retrieves a list of jobs for the user or,
#    if specified, the content owner that the user is acting on behalf of.
#    Then it prompts the user to select a job, and then executes case 2 and
#    then case 1.
# Usage examples:
# python retrieve_reports.py --content_owner_id=<CONTENT_OWNER_ID> --local_file=<LOCAL_FILE>
# python retrieve_reports.py --content_owner_id=<CONTENT_OWNER_ID> --job_id=<JOB_ID> --local_file=<LOCAL_FILE>
# python retrieve_reports.py --content_owner_id=<CONTENT_OWNER_ID> --report_url=<REPORT_URL> --local_file=<LOCAL_FILE>
#
###

from apiclient.discovery import build
from oauth2client.client import flow_from_clientsecrets
from oauth2client.tools import run_flow
from oauth2client.file import Storage
import httplib2

# The client_secrets_file variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret. You can acquire an OAuth 2.0 client ID and client secret from
# the {{ Google Cloud Console }} at
# {{ https://cloud.google.com/console }}.
# Please ensure that you have enabled the YouTube Data API for your project.
# For more information about using OAuth2 to access the YouTube Data API, see:
#   https://developers.google.com/youtube/v3/guides/authentication
# For more information about the client_secrets.json file format, see:
#   https://developers.google.com/api-client-library/python/guide/aaa_client_secrets


# Authorize the request and store authorization credentials.
def get_authenticated_service(client_secrets_file, scopes, api_service_name, api_version):

    flow = flow_from_clientsecrets(client_secrets_file, scope=scopes, message=' f off ')
    credentials_file = 'youtube-extraction.json' # e.g., projectName-oauth2.json
    storage = Storage(credentials_file)
    credentials = storage.get()   # Returns None if the file doesn't exist
    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage)  # This creates the credentials_file

    return build(api_service_name,  api_version,  http=credentials.authorize(httplib2.Http()))