#!/usr/bin/env python
import sys
import shutil
from typing import Optional, List, Tuple, Dict

import typer
from rich import print
from rich.columns import Columns
from rich.console import Console
from rich.traceback import install

# fmt: off
# Mapping from topics to colors
TOPICS = {
    "TIMR": "#9a9a99",
    "VOTE": "#67a0b2",
    "LEAD": "#d0b343",
    "TERM": "#70c43f",
    "LOG1": "#4878bc",
    "LOG2": "#398280",
    "CMIT": "#98719f",
    "PERS": "#d08341",
    "SNAP": "#FD971F",
    "DROP": "#ff615c",
    "CLNT": "#00813c",
    "RPC": "#fe2c79",
    "INFO": "#ffffff",
    "STATE": "#d08341",
    "ERRO": "#fe2626",
    "CONSIST": "#fe2626",
    "REPL": "#398280",
}
# fmt: on


def list_topics(value: Optional[str]):
    if value is None:
        return value
    topics = value.split(",")
    for topic in topics:
        topic = topic.strip().upper()
        if topic not in TOPICS:
            raise typer.BadParameter(f"topic {topic} not recognized")
    return topics


def main(
    file: typer.FileText = typer.Argument(None, help="File to read, stdin otherwise"),
    colorize: bool = typer.Option(True, "--no-color"),
    show_time: bool = typer.Option(True, "--no-time"),
    n_columns: Optional[int] = typer.Option(None, "--columns", "-c"),
    ignore: Optional[str] = typer.Option(None, "--ignore", "-i", callback=list_topics),
    just: Optional[str] = typer.Option(None, "--just", "-j", callback=list_topics),
    until: Optional[int] = typer.Option(-1, "--until", "-u")
):
    topics = list(TOPICS)

    # We can take input from a stdin (pipes) or from a file
    input_ = file if file else sys.stdin
    # Print just some topics or exclude some topics (good for avoiding verbose ones)

    if just is not None:
        topics = just
    if ignore is not None:
        topics = [lvl for lvl in topics if lvl not in set(ignore)]

    topics = set(topics)
    console = Console()
    width = console.size.width

    if show_time:
        n_columns = n_columns + 1

    panic = False
    for line in input_:
        try:
            time, topic, server_idx, msg = line.strip().split("|")
             
            time = time.strip()
            topic = topic.strip()

            if until > 0 and float(time) > until:
                break

            server_idx = int(server_idx.strip())
            # To ignore some topics
            if topic not in topics:
                continue

            # msg = " ".join(msg)
            msg = msg.strip()

            # Debug calls from the test suite aren't associated with
            # any particular peer. Otherwise we can treat second column
            # as peer id
            if topic != "TEST":
                # i = int(msg[1])
                i = server_idx
                

            # Colorize output by using rich syntax when needed
            if colorize and topic in TOPICS:
                color = TOPICS[topic]
                msg = f"[{color}]{msg}[/{color}]"

            # Single column printing. Always the case for debug stmts in tests
            if n_columns is None or topic == "TEST":
                print(time, msg)
            # Multi column printing, timing is dropped to maximize horizontal
            # space. Heavylifting is done through rich.column.Columns object
            else:
                cols = ["" for _ in range(n_columns)]
                """ if show_time:
                    cols[0] = time """
                msg = "" + msg
                if show_time:
                    cols[i+1] = msg
                    cols[0] = time
                else:
                    cols[i] = msg
                col_width = int(width / (n_columns))
                
                cols = Columns(cols, width=col_width - 1, equal=True, expand=True)
                #cols = Columns(cols, width=col_width - 1, expand=True)
                print(cols)
        except:
            #print(e.with_traceback())
            # Code from tests or panics does not follow format
            # so we print it as is
            if line.startswith("panic"):
                panic = True
            # Output from tests is usually important so add a
            # horizontal line with hashes to make it more obvious
            if not panic:
                print("#" * console.width)
            print(line, end="")


def run():
    typer.run(main)

if __name__ == "__main__":
    typer.run(main)