import logging
import pandas as pd
import numpy as np
import apiclient.discovery
import os



class VideosHandler:
    def __init__(self, youtube_data: apiclient.discovery, content_owner_id: str, report_folder: str) -> None:
        """
        Inicialização da classe

        Args:
            youtube_data (apiclient.discovery.object): Objeto da conexão com a API do Youtube
            content_owner_id (str): Id do content owner da conta do Youtube
            report_folder (str): Caminho da pasta onde estão armazenados os relatórios
        """
        self._youtube_data = youtube_data
        self._content_owner_id = content_owner_id
        self._report_folder = report_folder

    def _list_videos(self) -> np.array:
        """
        Função para listar os ids únicos de vídeos dentro do arquivo final de relatórios

        Returns:
            np.array: Array com os ids únicos do arquivo final de relatórios
        """

        # Get top 50 videos in revenue amount
        logging.info('Listing video ids from processed revenue report files')
        df_array = []
        for file in os.scandir(self._report_folder):
            if 'processed' in file.name:
                logging.info(f'Reading {file.name}')
                df = pd.read_csv(file)
                df_agg = (
                    df.groupby(["video_id"], as_index=False)[
                        "estimated_youtube_ad_revenue"
                    ]
                    .sum()
                    .sort_values(
                        by="estimated_youtube_ad_revenue",
                        ascending=False,
                        ignore_index=True,
                    )
                    .head(50)
                )
                ids = df_agg["video_id"].to_list()
                
                # Filter report to top 50 reports
                df = df[df["video_id"].isin(ids)]
                df = df.video_id.unique()
                df_array.append(df)

        logging.info('Finished listing video ids')
        df = np.concatenate(df_array)
        df = np.unique(df)
        return df

    def get_videos(self) -> list:
        """
        Função para listar vídeos do Youtube

        Returns:
            list: Lista de vídeos, contendo nome, id e id da categoria do vídeo
        """
        video_ids = self._list_videos()
        video_list = []
        size = len(video_ids)

        logging.info(f"Length of videos in date range {size}")
        logging.info("Started listing videos")
        for i in range(50, size + 50, 50):
            ids = ",".join(video_ids[i - 50 : i])
            logging.info(
                "Video ids from {} to {} total {}, {:.2f}%".format(
                    i - 50, i, size, 100 * i / size
                )
            )
            videos = (
                self._youtube_data.videos()
                .list(
                    onBehalfOfContentOwner=self._content_owner_id,
                    part="snippet",
                    id=ids,
                )
                .execute()
            )
            for video in videos["items"]:
                video_list.append(
                    {
                        "id": video["id"],
                        "name": video["snippet"]["title"].replace("\n", ""),
                        "categoryId": video["snippet"]["categoryId"],
                    }
                )

        logging.info("Videos listed")
        return video_list

    def get_categories(self, videos: pd.DataFrame) -> list:
        """
        Função para extrair categorias dos vídeos listados

        Args:
            videos (pd.DataFrame): Dataframe contendo os ids dos vídeos

        Returns:
            list: Lista das categorias dos vídeos listados
        """
        categories_list = []
        categories_ids = videos.categoryId.unique()
        categories_ids = [str(i) for i in categories_ids]
        categories_ids = ",".join(categories_ids)
        categories = (
            self._youtube_data.videoCategories()
            .list(part="snippet", id=categories_ids)
            .execute()
        )

        logging.info("Started listing videos")
        for category in categories["items"]:
            categories_list.append(
                {"id": category["id"], "name": category["snippet"]["title"]}
            )

        logging.info("Video categories listed")
        return categories_list
