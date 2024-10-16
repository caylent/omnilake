# OmniLake

OmniLake is a centralized data repository system built to support AI initiatives. It provides a scalable and efficient way to ingest, store, process, and retrieve information, enabling powerful AI-driven querying and analysis.

## Features

- Modular, event-driven architecture for scalability and flexibility
- Efficient vector storage and retrieval using LanceDB and S3
- AI-powered data compaction and summarization
- Flexible archive and entry management for organized data storage
- Robust job handling for asynchronous operations
- Semantic search capabilities using vector embeddings
- Integration with Amazon Bedrock for advanced AI functionalities
- Automated vector store management and optimization

## Overview

1. Architecture
    1. Modular design with separate services for API handling, data ingestion, storage management, and response generation
    1. Event-driven architecture using AWS EventBridge for asynchronous processing
    1. Serverless implementation leveraging AWS Lambda for scalability and cost-efficiency

1. Data Ingestion
    1. Processes incoming data, extracting metadata and insights
    1. Chunks large text documents for efficient storage and retrieval
    Generates vector embeddings for semantic search capabilities
    1. Supports various source types (e.g., files, websites, transcripts)

1. Storage
    1. Uses DynamoDB for metadata storage (archives, entries, jobs, etc.)
    1. Implements vector storage using LanceDB and S3 for efficient similarity search
    1. Manages multiple vector stores per archive for optimized performance
    1. Includes automatic rebalancing of vector stores based on content and usage patterns

1. Information Retrieval
    1. Provides semantic search capabilities using vector embeddings
    1. Implements a multi-stage compaction process for summarizing large amounts of information
    1. Generates AI-powered responses to user queries using language models (via Amazon Bedrock)

1. Job Management
    1. Tracks and manages asynchronous operations throughout the system
    1. Supports long-running tasks like data ingestion, vector store rebalancing, and response generation

1. API and Client Library
    1. Offers a REST API for external interactions
    1. Provides a Python client library for easy integration with other applications

1. Key Concepts
    1. Archives: Logical groupings of related data
    1. Entries: Individual pieces of content within archives
    1. Sources: Track the origin and provenance of data
    1. Vector Stores: Efficient storage and retrieval of vector embeddings

1. AI Integration
    1. Uses Amazon Bedrock for generating vector embeddings
    1. Leverages large language models for content summarization and response generation
    1. Implements AI-driven insights extraction from ingested content

1. Scalability and Maintenance:
    1. Includes automated processes for vector store management and optimization
    1. Implements maintenance modes for archives during large-scale operations
    1. Provides mechanisms for recalculating and updating metadata and tags

1. Development and Deployment:
    1. Uses AWS CDK for infrastructure-as-code and deployment
    1. Implements a development environment setup script (dev.sh) for easy onboarding
    1. Uses Poetry for Python dependency management

## Components

### API Service
Handles external interactions and manages the public-facing API.

### Ingestion Service
Processes new data entries, extracting metadata and preparing content for storage.

### Storage Service
Manages vector stores and data persistence, including rebalancing and optimization.

### Responder Service
Handles information requests, generating AI-powered responses based on stored data.

## Key Concepts

- **Archives**: Logical groupings of related data
- **Entries**: Individual pieces of content within archives
- **Sources**: Tracking of data provenance
- **Jobs**: Management of asynchronous processing tasks

## Technology Stack

- AWS Services: DynamoDB, S3, EventBridge, Lambda
- Vector Storage: LanceDB
- AI/ML: Amazon Bedrock for embeddings and language model inference
- Infrastructure: AWS CDK for deployment

## Getting Started

### Prerequisites

- Python 3.12 or higher
- Poetry (Python package manager)
- AWS CLI configured with appropriate credentials

### Installation

1. Clone the repository:
```
git clone https://github.com/your-repo/omnilake.git
cd omnilake
```

2. Install dependencies using Poetry:
```
poetry install
```

3. Set up the development environment:
```
./dev.sh
```

This script sets up necessary environment variables and prepares your local development environment.

## Usage

Here's a basic example of how to use the OmniLake client library to interact with the system:

```python
from omnilake.client.client import OmniLake
from omnilake.client.request_definitions import AddEntry, CreateArchive

# Initialize the OmniLake client
omnilake = OmniLake()

# Create a new archive
archive_req = CreateArchive(
    archive_id='my_archive',
    description='My first OmniLake archive'
)
omnilake.create_archive(archive_req)

# Add an entry to the archive
entry_req = AddEntry(
    archive_id='my_archive',
    content='This is a sample entry in my OmniLake archive.',
    sources=['https://example.com/source']
)
result = omnilake.add_entry(entry_req)

print(f"Entry added with ID: {result.response_body['entry_id']}")

# Request information
info_req = InformationRequest(
    archive_id='my_archive',
    goal='Summarize the contents of the archive',
    request='What information is stored in this archive?',
    request_type='INCLUSIVE'
)
response = omnilake.request_information(info_req)

print(f"Information request submitted. Job ID: {response.response_body['job_id']}")
```