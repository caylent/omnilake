from omnilake.client.commands.base import Command
import os
import shutil

class CreateConstructCommand(Command):
    command_name = 'create-construct'
    description = 'Generate a new construct (archive, processor, responder)'

    @classmethod
    def configure_parser(cls, parser):
        construct_parser = parser.add_parser(cls.command_name, help=cls.description)
        construct_parser.add_argument('--name', required=True, help='Name of the new construct')
        construct_parser.add_argument('--type', required=True, choices=['archive', 'processor', 'responder'], help='Type of the construct to create')

    def run(self, args):
        construct_name = args.name
        construct_type = args.type
        current_dir = os.getcwd()

        # Determine the correct directory structure based on current directory context
        dir_parts = current_dir.strip(os.sep).split(os.sep)

        if dir_parts[-2:] == ['constructs', f'{construct_type}s']:
            # User is in constructs/{type}s directory
            target_dir = os.path.join(current_dir, construct_name)

        elif dir_parts[-1] == 'constructs':
            # User is in constructs directory
            type_dir = os.path.join(current_dir, f'{construct_type}s')
            os.makedirs(type_dir, exist_ok=True)
            target_dir = os.path.join(type_dir, construct_name)

        else:
            # User is somewhere else
            type_dir = os.path.join(current_dir, 'constructs', f'{construct_type}s')
            os.makedirs(type_dir, exist_ok=True)
            target_dir = os.path.join(type_dir, construct_name)

        # Locate the template directory relative to command definition file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(script_dir, '../../../..'))
        template_dir = os.path.join(project_root, 'omnisa', 'constructs', f'{construct_type}s', '_template')

        if not os.path.exists(template_dir):
            print(f"❌ Template directory not found at '{template_dir}'. Please create the '_template' directory first.")
            return

        if os.path.exists(target_dir):
            print(f"❌ Target directory '{target_dir}' already exists.")
            return

        shutil.copytree(template_dir, target_dir)
        print(f"✅ Construct '{construct_name}' ({construct_type}) created at: {target_dir}")
