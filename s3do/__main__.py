#!/usr/bin/env python
import logging
import sys
from typing import Annotated, Optional

import typer
from click import Context
from typer.core import TyperGroup

from s3do.commands.inventory import inventory_command
from s3do.commands.tag import tag_command


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


@cli.command(help="List inventory from a bucket")
def inventory(
        bucket: Annotated[Optional[str], typer.Argument()] = None,
        prefix: Annotated[Optional[str], typer.Argument()] = None,
        symlink_file: Annotated[
            str, typer.Option('--symlink-file', '-s')
        ] = None
):
    inventory_command(bucket, prefix, symlink_file)


@cli.command(help="Tag S3 objects")
def tag(
        tags: Annotated[
            list[str], typer.Option('--tag', '-t')
        ],
        bucket: Annotated[Optional[str], typer.Argument()] = None,
        prefix: Annotated[Optional[str], typer.Argument()] = None,
        symlink_file: Annotated[
            str, typer.Option('--symlink-file', '-s')
        ] = None
):
    tag_command(bucket, prefix, symlink_file, tags)


if __name__ == '__main__':
    logging.basicConfig(format='[%(asctime)s] %(levelname)s: %(message)s')
    cli()
