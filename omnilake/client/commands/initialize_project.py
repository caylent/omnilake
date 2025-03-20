from omnilake.client.commands.base import Command
import os

class InitializeProjectCommand(Command):
    command_name = 'initialize_project'
    description = 'Initialize a new OmniSA project with default scaffolding'

    @classmethod
    def configure_parser(cls, parser):
        init_parser = parser.add_parser(cls.command_name, help=cls.description)
        init_parser.add_argument('--name', required=True, help='Name of the new project')

    def run(self, args):
        project_name = args.name
        base_dir = os.path.join(os.getcwd(), project_name)

        dirs_to_create = [
            'constructs/archives/_template/runtime',
            'constructs/processors/_template/runtime',
            'constructs/responders/_template/runtime',
            'examples',
        ]

        files_to_create = {
            '.gitignore': '__pycache__/\n*.pyc\n.env\n.venv\n',
            'README.md': f'# {project_name}\n\nOmniSA Project Initialized.',
            'dev.sh': '#!/bin/bash\n\nexport OMNILAKE_APP_NAME=\'{0}\'\nexport OMNILAKE_DEPLOYMENT_ID=\'dev\''.format(project_name),
            'pyproject.toml': '[tool.poetry]\nname = "{0}"\nversion = "0.1.0"\ndescription = ""\nauthors = []\n\n[tool.poetry.dependencies]\npython = "^3.12"\n\n[build-system]\nrequires = ["poetry-core"]\nbuild-backend = "poetry.core.masonry.api"'.format(project_name),
            'constructs/archives/_template/runtime/Dockerfile': 'ARG IMAGE\nFROM $IMAGE\nCOPY ./* ${LAMBDA_TASK_ROOT}/\n',
            'constructs/archives/_template/runtime/main.py': 'def handler(event, context):\n    return {"statusCode":200, "body":"Archive Template"}\n',
            'constructs/archives/_template/schemas.py': '# Define your schemas here\n',
            'constructs/archives/_template/stack.py': '# Define your stack here\n',
            'constructs/processors/_template/runtime/Dockerfile': 'ARG IMAGE\nFROM $IMAGE\nCOPY ./* ${LAMBDA_TASK_ROOT}/\n',
            'constructs/processors/_template/runtime/main.py': 'def handler(event, context):\n    return {"statusCode":200, "body":"Processor Template"}\n',
            'constructs/processors/_template/schemas.py': '# Define your schemas here\n',
            'constructs/processors/_template/stack.py': '# Define your stack here\n',
            'constructs/responders/_template/runtime/Dockerfile': 'ARG IMAGE\nFROM $IMAGE\nCOPY ./* ${LAMBDA_TASK_ROOT}/\n',
            'constructs/responders/_template/runtime/main.py': 'def handler(event, context):\n    return {"statusCode":200, "body":"Responder Template"}\n',
            'constructs/responders/_template/schemas.py': '# Define your schemas here\n',
            'constructs/responders/_template/stack.py': '# Define your stack here\n',
        }

        # Create directories
        for dir_path in dirs_to_create:
            full_path = os.path.join(base_dir, dir_path)
            os.makedirs(full_path, exist_ok=True)

        # Create files with default content
        for file_path, content in files_to_create.items():
            full_path = os.path.join(base_dir, file_path)
            with open(full_path, 'w') as file:
                file.write(content)

        # Make dev.sh executable
        os.chmod(os.path.join(base_dir, 'dev.sh'), 0o755)

        print(f"✅ Project '{project_name}' successfully initialized at {base_dir}.")
