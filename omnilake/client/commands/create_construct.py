from omnilake.client.commands.base import Command
import os
import shutil

class CreateConstructCommand(Command):
    command_name = 'create-construct'
    description = 'Generate a new construct (archive, processor, responder) from a basic template'

    @classmethod
    def configure_parser(cls, parser):
        construct_parser = parser.add_parser(cls.command_name, help=cls.description)
        construct_parser.add_argument('--name', required=True, help='Name of the new construct')
        construct_parser.add_argument('--type', required=True, choices=['archive', 'processor', 'responder'], help='Type of the construct to create')

    def run(self, args):
        construct_name = args.name
        construct_type = args.type
        current_dir = os.getcwd()

        # Determine correct directory
        dir_parts = current_dir.strip(os.sep).split(os.sep)

        if dir_parts[-2:] == ['constructs', f'{construct_type}s']:
            target_dir = os.path.join(current_dir, construct_name)
        elif dir_parts[-1] == 'constructs':
            type_dir = os.path.join(current_dir, f'{construct_type}s')
            os.makedirs(type_dir, exist_ok=True)
            target_dir = os.path.join(type_dir, construct_name)
        else:
            type_dir = os.path.join(current_dir, 'constructs', f'{construct_type}s')
            os.makedirs(type_dir, exist_ok=True)
            target_dir = os.path.join(type_dir, construct_name)

        # Corrected path calculation
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(script_dir, '../../..'))
        template_dir = os.path.join(project_root, 'omnilake', 'constructs', f'{construct_type}s', 'basic')

        if not os.path.exists(template_dir):
            print(f"❌ Basic template not found at '{template_dir}'. Please ensure it exists.")
            return

        if os.path.exists(target_dir):
            print(f"❌ Target directory '{target_dir}' already exists.")
            return

        shutil.copytree(template_dir, target_dir)
        self._hollow_out_construct(target_dir)

        print(f"✅ Construct '{construct_name}' ({construct_type}) created at: {target_dir}")

    def _hollow_out_construct(self, construct_path):
        runtime_path = os.path.join(construct_path, 'runtime')

        files_to_hollow = {
            'lookup.py': "Defines how to retrieve data entries. Replace with logic specific to your archive's storage or retrieval mechanisms.",
            'index.py': "Implements indexing logic for entries. Customize according to your archive's indexing requirements.",
            'provisioner.py': "Handles setup and provisioning of resources. Modify with infrastructure setup specific to your archive.",
            'generate_tags.py': "Responsible for generating metadata tags. Adjust based on your construct's metadata requirements or remove if unnecessary.",
            'event_definitions.py': "Defines events your construct emits or consumes. Customize or remove based on your event-driven architecture needs."
        }

        for filename, explanation in files_to_hollow.items():
            file_path = os.path.join(runtime_path, filename)
            if os.path.exists(file_path):
                with open(file_path, 'w') as f:
                    f.write(f'# TODO: {explanation}\n')

        schemas_path = os.path.join(construct_path, 'schemas.py')
        if os.path.exists(schemas_path):
            with open(schemas_path, 'w') as f:
                f.write('# TODO: Define schemas for configuration, requests, and responses tailored to your construct.\n')

        stack_path = os.path.join(construct_path, 'stack.py')
        if os.path.exists(stack_path):
            with open(stack_path, 'w') as f:
                f.write('# TODO: Define AWS CDK stack resources, permissions, and infrastructure specific to your construct.\n')
