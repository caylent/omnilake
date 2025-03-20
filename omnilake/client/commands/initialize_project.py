import os
from omnilake.client.commands.base import Command

class InitializeProjectCommand(Command):
    command_name = 'initialize_project'
    description = 'Initialize a new OmniSA project with default scaffolding'

    @classmethod
    def configure_parser(cls, parser):
        init_parser = parser.add_parser(cls.command_name, help=cls.description)
        init_parser.add_argument('--name', required=True, help='Name or path of the new project')

    def run(self, args):
        project_path = os.path.abspath(args.name)
        dirs_to_create = [
            'constructs/archives',
            'constructs/processors',
            'constructs/responders',
            'examples',
            'stacks'
        ]

        files_to_create = {
            '.gitignore': '__pycache__/\n*.pyc\n.env\n.venv\n',
            'README.md': f'# {os.path.basename(project_path)}\n\n Project Initialized.',
            'dev.sh': f'#!/bin/bash\n\nexport OMNILAKE_APP_NAME="{os.path.basename(project_path)}"\nexport OMNILAKE_DEPLOYMENT_ID="dev"\n',
            'pyproject.toml': (
                f'[tool.poetry]\n'
                f'name = "{os.path.basename(project_path)}"\n'
                'version = "0.1.0"\n'
                'description = ""\n'
                'authors = []\n\n'
                '[tool.poetry.dependencies]\n'
                'python = "^3.12"\n\n'
                '[build-system]\n'
                'requires = ["poetry-core"]\n'
                'build-backend = "poetry.core.masonry.api"\n'
            ),
        }

        # Create base directories
        for dir_path in dirs_to_create:
            full_path = os.path.join(project_path, dir_path)
            os.makedirs(full_path, exist_ok=True)

        # Create project files
        for file_path, content in files_to_create.items():
            full_path = os.path.join(project_path, file_path)
            with open(full_path, 'w') as file:
                file.write(content)

        # Set dev.sh executable
        os.chmod(os.path.join(project_path, 'dev.sh'), 0o755)

        print(f"\n✅ Project successfully initialized at: {project_path}")
        print("\nNext steps:")
        print(f"👉 Navigate to your new project:\ncd {project_path}")
        print("👉 Create your first archive construct:")
        print("poetry run omni create-construct --name my_first_archive --type archive\n")
