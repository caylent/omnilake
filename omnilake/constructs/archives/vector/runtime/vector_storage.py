"""
Vector Storage Definitions
"""
import logging

from dataclasses import dataclass, field
from typing import List 

from lancedb.pydantic import LanceModel, Vector


class DocumentChunk(LanceModel):
    """
    Document chunk model.
    """
    entry_id: str
    chunk_id: str
    vector: Vector(dim=1024) # type: ignore


@dataclass
class VectorRankingItem:
    vector_storage_id: str
    tags: List[str] = field(default_factory=list)

    def calculate_match(self, expected_tags: List[str]) -> int:
        """
        Calculate the match between the expected tags and the tags of the vector.

        Keyword arguments:
        expected_tags -- The expected tags to rank the vector against.
        """
        logging.debug(f"Calculating match between {expected_tags} and {self.tags}")

        return len(set(expected_tags) & set(self.tags)) / len(expected_tags)


def vector_ranker(expected_tags: List[str], items: List[VectorRankingItem], max_length: int = 1) -> List[VectorRankingItem]:
    """
    Rank the items based on the expected tags.

    Keyword arguments:
    expected_tags -- The expected tags to rank the vectors against.
    items -- The items to rank.
    max_length -- The maximum number of items to return.
    """
    ranked_items = sorted(items, key=lambda item: item.calculate_match(expected_tags), reverse=True)

    return ranked_items[:max_length]


def calculate_tag_match_percentage(object_tags: List[str], target_tags: List[str]) -> int:
    """
    Calculate the match percentage between the object's tags and the target tags.

    Keyword arguments:
    object_tags -- The list of tags to compare
    target_tags -- The list of tags to compare
    """
    matching_tags = set(object_tags) & set(target_tags)

    # Calculate the match percentage
    return len(matching_tags) / len(target_tags) * 100