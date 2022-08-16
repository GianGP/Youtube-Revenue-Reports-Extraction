import logging
from apiclient.http import MediaIoBaseDownload
from io import FileIO
from urllib.error import HTTPError
import pandas as pd
import os
from datetime import datetime, timedelta
import apiclient.discovery
import json


class ReportsHandler:
    def __init__(
        self,
        youtube_reporting: apiclient.discovery,
        content_owner_id: str,
        job_id: str,
        last_date: str,
        start_date: str,
        report_schema: dict,
        report_name: str,
        report_folder: str,
    ) -> None:
        """
        Inicialização da classe

        Args:
            youtube_reporting (apiclient.discovery.object): Objeto da conexão com a API do Youtube
            content_owner_id (str): Id do content owner da conta do Youtube
            job_id (str): Id do job dos relatórios do Youtube
            last_date (str): Última data em que foi executada a extração
            start_date (str): Primeira data de dados
            report_schema (dict): Schema dos dados do relatório
            report_name (str): Nome do job dos relatórios do Youtube
            report_folder (str): Caminho da pasta dos relatórios baixados
        """
        self._youtube_reporting = youtube_reporting
        self._content_owner_id = content_owner_id
        self._last_date = last_date
        self._job_id = job_id
        self._columns = self._get_columns_from_schema(report_schema)
        self._report_name = report_name
        self._temp_folder = report_folder
        self._start_date = start_date

    def _get_columns_from_schema(self, report_schema: dict) -> list:
        """
        Função para extrair o nome das colunas do relatório a partir do seu schema

        Args:
            report_schema (dict): Schema das colunas do relatório

        Returns:
            list: Lista com os nomes das colunas do relatório
        """
        return [column["name"] for column in report_schema]

    # Call the YouTube Reporting API's reports.list method to retrieve reports created by a job.
    def _retrieve_reports(self) -> list:
        """
        Função para extrair todos os relatórios do youtube para o job selecionado, para o Content Owner definido e
        criados após a última data analisada

        Returns:
            list: lista de dicts contendo as principais informações sobre os relatórios
        """
        logging.info('Retrieving reports')
        results = (
            self._youtube_reporting.jobs()
            .reports()
            .list(
                jobId=self._job_id,
                onBehalfOfContentOwner=self._content_owner_id,
                createdAfter=self._last_date,
            )
            .execute()
        )
        logging.info("Reports retrieved")

        if "reports" in results and results["reports"]:
            reports = []
            for report in results["reports"]:
                if report["startTime"] >= self._start_date:
                    reports.append(
                        {
                            "url": report["downloadUrl"],
                            "created_date": report["createTime"],
                            "date": report["startTime"],
                        }
                    )
            return reports

    # If there's more than one report with same date but different created_date
    # get only the latest report
    def _filter_reports(self, reports: dict) -> dict:
        """
        Função para filtrar os relatórios que vem da API
        
        De acordo com a doc da API do Youtube, relatórios de receita podem ser reprocessados
        dias depois de seu primeiro processamento. Para pegar apenas os relatórios mais recentes
        essa função percorre todos os relatórios que vieram da API e pega apenas a versão mais
        recente para cada data

        Args:
            reports (dict): Relatórios vindo do youtube

        Returns:
            dict: Relatórios filtrados
        """
        df = pd.DataFrame(reports)
        df_filter = df.groupby(["date"], as_index=False)["created_date"].max()
        df = pd.merge(df_filter, df.drop_duplicates())
        return df.to_dict("records")

    # Call the YouTube Reporting API's media.download method to download the report.
    def _download_report(self, report_url: str, local_file: str) -> None:
        """
        Função para baixar os relatórios listados na API

        Args:
            report_url (str): url do relatório
            local_file (str): nome do arquivo local em que o relatório será salvo
        """
        request = self._youtube_reporting.media().download(resourceName=" ")
        request.uri = report_url

        fh = FileIO(local_file, mode="wb")
        # Stream/download the report in a single request.
        downloader = MediaIoBaseDownload(fh, request, chunksize=-1)

        done = False
        while done is False:
            status, done = downloader.next_chunk()
            if status:
                logging.info(
                    f"Download of {local_file} {int(status.progress() * 100)}%."
                )
        logging.info(f"Download of {local_file} Complete!")

    def _process_revenue_reports(self) -> None:
        """
        Função para processar os relatórios de receita, calculando valores mais precisos
        para o estimated_youtube_ad_revenue
        """
        for file in os.scandir(self._temp_folder):
            if 'raw' in file.name:
                df = pd.read_csv(file)

                # Formats columns
                df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
                df["estimated_youtube_ad_revenue"] = (df["ad_impressions"] * (df["estimated_cpm"])) / 1000
                df["is_self_uploaded"] = df["uploader_type"] == "self"
                df = df[df.columns.intersection(self._columns)]
                
                # Save as csv
                file_name = '-'.join(file.name.split("-")[1:])
                df.to_csv(f"./{self._temp_folder}/processed-{file_name}", index=False)
                logging.info(f"Report processed-{file.name} processed")

    def _update_report_date(self) -> None:
        """
        Função para atualizar a última data de processamento
        """
        last_date = datetime.today().strftime("%Y-%m-%dT%H:%M:%SZ")
        with open("config.json", "r") as jsonfile:
            data = json.load(jsonfile)
            data["REPORTING"]["LAST_DATE"] = last_date
        
        with open("config.json", 'w') as jsonfile:
            json.dump(data, jsonfile, indent=4)
            logging.info("Date updated")

    def run_reports(self) -> None:
        """
        Função para executar a sequência dos relatórios
        """
        try:
            reports = self._retrieve_reports()
            reports = self._filter_reports(reports)
            for report in reports:
                # Download the selected report.
                if report:
                    local_file = f'{self._temp_folder}/raw-{self._report_name}-{report["date"].split("T")[0].replace("-","")}.csv'
                    self._download_report(report["url"], local_file)

            self._process_revenue_reports()
            self._update_report_date()
        except HTTPError as e:
            logging.error("An HTTP error %d occurred:\n%s" % (e.resp.status, e.content))
