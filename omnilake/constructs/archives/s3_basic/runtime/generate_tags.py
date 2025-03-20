import json
import urllib.parse

def handler(event, context):
    # Extract event data
    config = event['configuration']
    prefix = config.get('prefix', '')
    object_key = event['index_key']

    # Construct full key using optional prefix
    full_key = f"{prefix.rstrip('/')}/{object_key}" if prefix else object_key
    decoded_key = urllib.parse.unquote_plus(full_key)

    tags = []

    # Tag based on file extension
    if '.' in decoded_key:
        file_extension = decoded_key.rsplit('.', 1)[-1].lower()
        tags.append(f'file_type:{file_extension}')

    # Additional tags from path components (excluding prefix)
    path_components = decoded_key.split('/')

    # If prefix exists, exclude its components from tagging
    prefix_components = prefix.strip('/').split('/') if prefix else []
    relevant_components = path_components[len(prefix_components):-1] if prefix else path_components[:-1]

    for folder in relevant_components:
        sanitized_folder = folder.strip().lower().replace(' ', '_')
        if sanitized_folder:
            tags.append(f'folder:{sanitized_folder}')

    print(f"✅ Generated tags for '{decoded_key}': {tags}")

    return {
        "statusCode": 200,
        "body": json.dumps({"tags": tags})
    }
