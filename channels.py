import logging
import apiclient.discovery


class ChannelsHandler:
    def __init__(self, youtube_data: apiclient.discovery, content_owner_id: str) -> None:
        """
        Inicialização da classe

        Args:
            youtube_data (apiclient.discovery.object): Objeto da conexão com a API do Youtube
            content_owner_id (str): Id do content owner da conta do Youtube
        """
        self._youtube_data = youtube_data
        self._content_owner_id = content_owner_id

    def _save_channels(self, page_token: str, channels_list: list) -> str:
        """
        Função para listar os canais relacionados ao self._content_owner_id

        *** Observação: o parâmetro "channels_list" é criado externamente e, a partir da referência da lista,
        *** preenchido nesta função

        Args:
            page_token (str): Token da página atual da API para listar canais no Youtube
            channels_list (list): Referência da lista com os canais do youtube

        Returns:
            str: Token da próxima página da API para listar canais no Youtube
        """

        # Second to last API executions, with page_token
        if page_token:
            channels = (
                self._youtube_data.channels()
                .list(
                    onBehalfOfContentOwner=self._content_owner_id,
                    managedByMe=True,
                    maxResults=50,
                    part="snippet",
                    pageToken=page_token,
                )
                .execute()
            )

        # First API execution, without page_token
        else:
            channels = (
                self._youtube_data.channels()
                .list(
                    onBehalfOfContentOwner=self._content_owner_id,
                    managedByMe=True,
                    maxResults=50,
                    part="snippet",
                )
                .execute()
            )

        for channel in channels["items"]:
            channel_dict = {"id": channel["id"], "name": channel["snippet"]["title"]}
            channels_list.append(channel_dict)

        return channels.get("nextPageToken")

    def get_channels(self) -> list:
        """
        Função para listar todos os canais do Youtube relacionados a um content_owner

        Returns:
            list: Lista de canais do youtube
        """
        channels_list = []
        page_token = self._save_channels(None, channels_list)

        logging.info("Started listing channels")
        while page_token:
            page_token = self._save_channels(page_token, channels_list)

        logging.info("Channels listed")

        return channels_list
