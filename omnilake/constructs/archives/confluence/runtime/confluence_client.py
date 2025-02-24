from atlassian import Confluence
import html2text


class ConfluencePageResponse(object):
    def __init__(self, page_id, content):
        self.page_id = page_id
        self.content = content


class ConfluenceClient(object):
    def __init__(self, url, username, api_key, space_key):
        self.url = url
        self.username = username
        self.api_key = api_key
        self.space_key = space_key
        self.confluence = Confluence(
            url=self.url,
            username=self.username,
            password=self.api_key
        )

    def _download_page_content(self, page):
        """
        Download and save the content of a Confluence page as HTML.
        :param page: A page dictionary object from Confluence API
        :return: The path where the HTML file is saved
        """
        page_id = page['id']

        # Retrieve page content in storage format (HTML)
        page_content = self.confluence.get_page_by_id(page_id, expand='body.storage')
        html_content = page_content['body']['storage']['value']
        clear_html_content = html2text.html2text(html_content)

        return ConfluencePageResponse(page_id=page_id, content=clear_html_content)


    def list_pages(self):
        pages = self.confluence.get_all_pages_from_space(self.space_key)
        content_result = []
        for page in pages:
            content_result.append(self._download_page_content(page))
        return content_result