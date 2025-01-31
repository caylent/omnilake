"""
Handles the Vector Storage queries
"""
import json
import logging
import math

from typing import List

import boto3
import lancedb

from da_vinci.core.global_settings import setting_value

from omnilake.tables.entries.client import Entry, EntriesClient
from omnilake.tables.indexed_entries.client import IndexedEntriesClient
from omnilake.constructs.archives.vector.tables.vector_stores.client import VectorStoresClient


class VectorStorageSearch:
    """
    Vector Storage Query
    """
    def __init__(self):
        self.storage_bucket_name = setting_value(
            namespace='omnilake::vector_storage',
            setting_key='vector_store_bucket',
        )

    def _query(self, db: lancedb.DBConnection, vector_store_id: str, query: str, result_limits: int = 100) -> List[str]:
        """
        Load the results from the Vector Storage service.

        Keyword arguments:
        db -- The connection to the Vector Storage service
        vector_store_id -- The ID of the vector store to query
        query -- The query to perform
        result_limits -- The number of results to return
        """
        table = db.open_table(name=vector_store_id)

        result = table.search(query).metric("cosine").limit(result_limits).to_list()

        entries = [r["entry_id"] for r in result]

        return entries

    def _remove_source_duplicates(self, entries: List[Entry]) -> List[Entry]:
        """
        Remove all entries that are duplicates of the source. Favor the entry with the latest effective date.

        Keyword arguments:
        entries -- The entries to remove duplicates from.
        """
        # Track all of the original source entries
        existing_source_entries = {}

        ids_to_remove = set()

        entries_client = EntriesClient()

        for idx, entry in enumerate(entries):
            # Fetch Entry Table Object
            entry_global_obj  = entries_client.get(entry_id=entry)

            original_of_source = entry_global_obj.original_of_source

            # If Entry is not original content of a source, skip
            if not original_of_source:
                continue

            # If the original source is not in the existing source entries, add it
            if original_of_source not in existing_source_entries:
                existing_source_entries[original_of_source] = {
                    'list_id': idx,
                    'effective_date': entry_global_obj.effective_on,
                }

                continue

            existing_entry_idx = existing_source_entries[original_of_source]['list_id']

            existing_entry_effective_date = existing_source_entries[original_of_source]['effective_date']

            existing_entry = entries[existing_entry_idx]

            if existing_entry_effective_date < entry_global_obj.effective_on:
                logging.debug(f'Removing duplicate source entry {existing_entry} in favor of {entry}.')

                ids_to_remove.add(existing_entry_idx)

                existing_source_entries[original_of_source]['list_id'] = idx

                existing_source_entries[original_of_source]['effective_date'] = entry_global_obj.effective_on

            else:
                logging.debug(f'Removing duplicate source entry {entry} in favor of {existing_entry}.')

                ids_to_remove.add(idx)

        return [entry for idx, entry in enumerate(entries) if idx not in ids_to_remove]

    def _sort_entries_by_tag(self, archive_id: str, entries: List[Entry], target_tags: List[str]) -> List[Entry]:
        """
        Sort the entries based on the target tags.

        Keyword arguments:
        archive_id -- The ID of the archive to sort against.
        entries -- The entry_ids to sort.
        target_tags -- The target tags to sort against.
        """
        indexed_entries = IndexedEntriesClient()

        entries_to_sort = []

        for entry in entries:
            logging.debug(f'Fetching entry ID: {entry}')

            entry_index = indexed_entries.get(
                archive_id=archive_id,
                entry_id=entry,
            )

            logging.debug(f'Entry index details: {entry_index}')

            if not entry_index:
                raise ValueError(f'Could not find entry index for {entry} in archive {archive_id}')

            entries_to_sort.append(entry_index)

        sorted_entries = sorted(
            entries_to_sort,
            key=lambda entry_obj: entry_obj.calculate_score(target_tags),
            reverse=True,
        )

        result = [entry.entry_id for entry in sorted_entries]

        return result

    @staticmethod
    def text_embedding(text: str):
        """
        Create a prompt embedding for the query.

        Keyword Arguments:
            prompt: The prompt to query
        """
        bedrock = boto3.client(service_name='bedrock-runtime')

        body = json.dumps({
            "texts": [text],
            "input_type": "search_query"
        })
        
        response = bedrock.invoke_model(
            modelId="cohere.embed-multilingual-v3",
            contentType="application/json",
            accept="application/json",
            body=body
        )
    
        response_body = json.loads(response['body'].read())

        logging.debug(f'Embedding response: {response_body}')

        embedding = response_body['embeddings'][0]

        return embedding

    def execute(self, archive_id: str, query_string: str, max_entries: int, prioritize_tags: List[str] = None) -> List[str]:
        """
        Entry point for the query API Lambda function.
        """
        query = self.text_embedding(query_string)

        vector_stores = VectorStoresClient()

        vector_store = vector_stores.get(archive_id=archive_id)

        if not vector_store:
            raise ValueError(f'Could not find vector store for archive {archive_id}')

        vector_store_id = vector_store.vector_store_id

        logging.info(f'Querying vector storage "{vector_store_id}" with "{query}"')

        db = lancedb.connect(f's3://{self.storage_bucket_name}')

        resulting_entries = self._query(
            db=db,
            query=query,
            result_limits=max_entries + math.ceil(max_entries * 0.3), # Set query limit to 30% more than the max entries
            vector_store_id=vector_store_id,
        )

        logging.info(f'Vector storage query returned {len(resulting_entries)} results.')

        de_duplicated_entries = self._remove_source_duplicates(entries=resulting_entries)

        if prioritize_tags:
            sorted_entries = self._sort_entries_by_tag(
                archive_id=archive_id,
                entries=de_duplicated_entries,
                target_tags=prioritize_tags,
            )

            finalized_entries = sorted_entries[:max_entries]

        else:
            finalized_entries = de_duplicated_entries[:max_entries]

        return finalized_entries