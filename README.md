OmniLake
========
OmniLake is a Python/AWS Framework that enables the development of enterprise-grade AI applications with built-in data lineage and traceability. It provides a comprehensive solution for managing unstructured information while addressing common AI adoption challenges.

### Key Features

- Built-in data lineage tracking for AI outputs
- Scalable from proof-of-concept to enterprise deployment
- Standardized data management with full control
- Rapid deployment capabilities with minimal initial setup
- Cost-effective scaling with pay-as-you-go model
- Advanced semantic search and retrieval
- Customizable for specific business needs

### Core Benefits

- Reduces AI implementation complexity
- Ensures traceability of AI-generated content
- Enables quick proof-of-concept development
- Provides enterprise-grade data management
- Maintains control over data

### ⚠️ Experimental Feature Warning ️⚠️

The automatic archive segmentation feature (which attempts to split and categorize information into purpose-built vector stores) is very experimental. While promising for knowledge organization, it's not yet production-ready and should be used with caution in non-critical environments.

Components
----------

### API Service
Handles external interactions and manages the public-facing API.

### Ingestion Service
Processes new data entries and prepares content for storage.

### Storage Service
Manages vector stores and data persistence, including rebalancing and optimization.

### Responder Service
Handles information requests, generating AI-powered responses based on stored data.

Key Concepts
------------

### Definitions

- **Archives**: Logical groupings of related data
- **Entries**: Individual pieces of content within archives
- **Sources**: Tracking of data provenance
- **Jobs**: Management of asynchronous processing tasks

### Technology

- AWS Services: DynamoDB, S3, EventBridge, Lambda
- Vector Storage: LanceDB
- AI/ML: Amazon Bedrock for embeddings and language model inference
- Infrastructure: AWS CDK for deployment

Getting Started
---------------
To utilize Omnilake effectively, you should include it as a dependency for your own application. However, the framework will deploy and create all the necessary services to start using a base Omnilake deployment.

### Prerequisites

- Python 3.12 or higher
- Poetry (Python package manager)
- [Da Vinci Framework](https://github.com/jarosser06/da-vinci)
- AWS CLI configured with appropriate credentials
- AWS Account (Max managed policies limit must be bumped to 20)

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
from omnilake.client.request_definitions import (
    AddEntry,
    AddSource,
    BasicInformationRetrievalRequest,
    CreateSourceType,
    CreateArchive,
    InformationRequest,
)

# Initialize the OmniLake client
omnilake = OmniLake()

# Create a new archive
archive_req = CreateArchive(
    archive_id='my_archive',
    description='My first OmniLake archive'
)
omnilake.create_archive(archive_req)

source_type = CreateSourceType(
    name='webpage',
    description='Content that belongs to a web page',
    required_fields=['url', 'published_date'],
)
omnilake.create_source_type(source_type)

source = AddSource(
    source_type='webpage',
    source_arguments={
        'url': 'https://example.com/about',
        'published_date': '2024-24-12',
    }
)
source_result = omnilake.add_source(source)

source_rn = source_result.response_body['resource_name']

# Add an entry to the archive
entry_req = AddEntry(
    archive_id='my_archive',
    content='This is a sample entry in my OmniLake archive.',
    sources=[source_rn],
    original_source=source_rn # Indicates whether the content is original content of the source location
)
result = omnilake.add_entry(entry_req)

print(f"Entry added with ID: {result.response_body['entry_id']}")

# Request information
info_req = InformationRequest(
    goal='Summarize the contents of the archive',
    retrieval_requests=[
        BasicInformationRetrievalRequest(
            archive_id='my_archive',
            max_entries=10,
        )
    ]
)
response = omnilake.request_information(info_req)

print(f"Information request submitted. Job ID: {response.response_body['job_id']}")
```