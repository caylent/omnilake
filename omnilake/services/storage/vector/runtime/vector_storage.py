'''
Vector Storage Definitions
'''
import logging

from dataclasses import dataclass, field
from typing import List 

from lancedb.pydantic import LanceModel, Vector

from omnilake.tables.vector_stores.client import VectorStoresClient
from omnilake.tables.vector_store_tags.client import VectorStoreTagsClient


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


def choose_vector_stores(archive_id: str, expected_tags: List[str]) -> List[str]:
    """
    Select vector stores based on the expected tags.

    Keyword arguments:
    archive_id -- The archive ID to select the vector store from.
    expected_tags -- The expected tags to rank the vector stores against.
    """
    logging.debug(f"Choosing vector store for archive {archive_id} with tags {expected_tags}")

    vector_store_tags = VectorStoreTagsClient()

    vector_store_ids = vector_store_tags.get_all_matching_vector_stores(
        archive_id=archive_id,
        tags=list(set(expected_tags))
    )

    vector_stores = VectorStoresClient()

    if vector_store_ids:
        vector_store_items = []

        # Get the vector store object
        for vector_store_id in vector_store_ids:
            vector_store = vector_stores.get(archive_id=archive_id, vector_store_id=vector_store_id)

            vector_store_items.append(vector_store)

        ranking_items = []

        # Get the tags for each vector store
        for vector_store in vector_store_items:
            store_tags = vector_store_tags.get_tags_for_vector_store(
                archive_id=archive_id,
                vector_store_id=vector_store.vector_store_id,
            )

            logging.debug(f"Vector store {vector_store.vector_store_id} has tags {store_tags}")

            ranking_items.append(VectorRankingItem(
                vector_storage_id=vector_store,
                tags=store_tags,
            ))

        ranking_results = vector_ranker(
            expected_tags=expected_tags,
            items=ranking_items,
        )

        return [res.vector_storage_id for res in ranking_results]

    else:
        logging.debug(f"No vector stores found for archive {archive_id} with tags {expected_tags}")

        vector_store_items = vector_stores.get_by_archive(archive_id)

        return [v_store.vector_store_id for v_store in vector_store_items]