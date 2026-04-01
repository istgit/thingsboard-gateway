import json
import os

from rich.console import Console

console = Console()

def initialize_devices():

    # Load configuration from dnp3_config_small_batch.json
    global config
    config_path = os.path.join(
        "/thingsboard_gateway/config",
        "dnp3_config_small_batch.json"
    )
    try:

        with open(config_path, 'r') as config_file:
            config = json.load(config_file)

        console.print(f"[bold green]Loaded configuration from {config_path}[/bold green]")
        return config
    except FileNotFoundError:
        console.print(f"[red]Configuration file {config_path} not found[/red]")
        return
    except json.JSONDecodeError as e:
        console.print(f"[red]Error decoding JSON from {config_path}: {str(e)}[/red]")
        return

