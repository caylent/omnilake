'''
AI Engineering Constructs to help control the response of the AI
'''
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from typing import List


@dataclass
class AIResponseInsightDefinition:
    """
    AI Response Insight Definition
    """
    name: str
    definition: str
    prompt_template: str = "- Insight Name: {name}\n\tDefinition: {definition}\n\t"

    def description(self) -> str:
        """
        Return the definition as a string
        """
        return self.prompt_template.format(
            name=self.name,
            definition=self.definition,
        )

    def to_dict(self) -> dict:
        """
        Return the object as a dictionary
        """
        return asdict(self)


@dataclass
class AIResponseDefinition:
    """
    AI Response Definition
    """
    insights: List[AIResponseInsightDefinition]
    prompt_template: str = """Your job is to review the given content and provide a response for each of the following insights:
{insight_definitions}

You should respond only using the following format:
<analysis>{response_structure}</analysis>

DO NOT include any other information in your response!

Content:
{content}
"""

    def to_prompt(self, content: str) -> str:
        """
        Return the prompt as a string
        """
        insight_definitions = "\n".join(
            [insight.description() for insight in self.insights]
        )

        response_substructure_template = """
    <{insight_name}>...</{insight_name}>
"""

        response_structure_lst = []

        for insight in self.insights:
            response_structure_lst.append(
                response_substructure_template.format(
                    insight_name=insight.name,
                )
            )

        response_structure = "\n".join(response_structure_lst)

        return self.prompt_template.format(
            insight_definitions=insight_definitions,
            response_structure=response_structure,
            content=content
        )

    def to_dict(self) -> dict:
        """
        Return the object as a dictionary
        """
        return asdict(self)


class ResponseParser(HTMLParser):
    def __init__(self):
        """
        Parse the output of a response from the LLM

        Parse:
        <analysis>
        ...
        </analysis>
        """
        super().__init__()

        self.top_level_tag = 'analysis'

        self.current_insight = None

        self.currently_reading = None

        self.values = {}

        self.reading_response = False

    def handle_starttag(self, tag, attrs):
        """
        Handle the start tag

        If the tag is not the top level tag, then we are reading the response

        Keyword arguments:
        tag -- the tag to handle
        attrs -- the attributes of the tag
        """
        if tag not in ('analysis'):
            self.currently_reading = tag

            self.reading_response = True

            self.current_insight = tag

    def handle_endtag(self, tag):
        """
        Handle the end tag

        If the tag is not the top level tag, then we are reading the response

        Keyword arguments:
        tag -- the tag to handle
        """
        if tag not in ('analysis'):
            self.currently_reading = None

            self.reading_response = False

            self.current_insight = None

    def handle_data(self, data):
        """
        Handle the data between tags

        If we are reading the response, then add the data to the current insight

        Keyword arguments:
        data -- the data to handle
        """
        if not self.reading_response:
            return

        if self.current_insight not in self.values:
            self.values[self.current_insight] = ''

        self.values[self.current_insight] += data

    def parser_not_empty(self) -> bool:
        """
        Check if the parser has any values
        """
        return bool(self.values)

    def parsed_insights(self):
        """
        Return the parsed insights
        """
        return self.values