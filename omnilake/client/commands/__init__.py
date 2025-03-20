'''
Omnitizer Commands

- Index: Indexes the omnilake database by crawling the given directory
- Question: Takes a question about the project and returns the answer
'''

from omnilake.client.commands.chain import ChainCommand
from omnilake.client.commands.index import RefreshIndexCommand
from omnilake.client.commands.question import QuestionCommand
from omnilake.client.commands.create_construct import CreateConstructCommand
from omnilake.client.commands.initialize_project import InitializeProjectCommand  # ← add this import


__all__ = {k.command_name: k for k in [
    ChainCommand,
    RefreshIndexCommand,
    QuestionCommand,
    CreateConstructCommand,
    InitializeProjectCommand
]}
