#!/usr/bin/env python
import logging
import sys
from typing import Annotated, Optional

import click
import typer
from click import Context
from typer.core import TyperGroup

from s3do import inventory, tag


class Unbuffered(object):
    def __init__(self, stream):
        self.stream = stream

    def write(self, data):
        self.stream.write(data)
        self.stream.flush()

    def writelines(self, datas):
        self.stream.writelines(datas)
        self.stream.flush()

    def __getattr__(self, attr):
        return getattr(self.stream, attr)


sys.stdout = Unbuffered(sys.stdout)


class OrderCommands(TyperGroup):
    def list_commands(self, ctx: Context):
        return list(self.commands)


cli = typer.Typer(no_args_is_help=True, add_completion=False)


@click.group()
def cli_entry_point():
    pass


@cli.command(help="Tag S3 objects")
def tag(
        bucket: Annotated[Optional[str], typer.Argument()] = None,
        prefix: Annotated[Optional[str], typer.Argument()] = None,
):
    tag.tag_command(bucket, prefix)


cli_entry_point.add_command(inventory.inventory)

if __name__ == '__main__':
    logging.basicConfig(format='[%(asctime)s] %(levelname)s: %(message)s')
    cli_entry_point()
