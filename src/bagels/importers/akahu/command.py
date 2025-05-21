from pathlib import Path
from datetime import datetime, timedelta
import click

from bagels.importers.akahu.importer import AkahuImporter
from bagels.locations import set_custom_root


now = datetime.now()
defaultEnd = now.strftime("%Y-%m-%dT%H:%M:%S")
defaultStart = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")


@click.command("akahu")
@click.argument("app_token", envvar="AKAHU_APP_TOKEN")
@click.argument("user_token", envvar="AKAHU_USER_TOKEN")
@click.option(
    "--start",
    default=defaultStart,
    type=click.DateTime(),
    help="Start date for transactions.",
)
@click.option(
    "--end",
    default=defaultEnd,
    type=click.DateTime(),
    help="End date for transactions.",
)
@click.option("--delete-categories", is_flag=True)
@click.pass_context
def akahu_importer(
    ctx,
    app_token: str | None,
    user_token: str | None,
    start: datetime | None,
    end: datetime | None,
    delete_categories: bool,
) -> None:
    """Import data from Akahu."""

    if not app_token or not user_token:
        click.echo(
            "Please set the AKAHU_APP_TOKEN and AKAHU_USER_TOKEN environment variables."
        )
        return

    importer = AkahuImporter(app_token, user_token)

    importer.run(start, end)

    if delete_categories:
        click.echo("Deleting categories...")
        importer.delete_categories()
        click.echo("Categories deleted.")


if __name__ == "__main__":

    @click.option(
        "--at",
        type=click.Path(exists=True, file_okay=True, dir_okay=True, path_type=Path),
        help="Specify the path.",
    )
    @click.pass_context
    def cli(ctx, at: Path | None, migrate: str | None, source: Path | None):
        """Akahu Importer"""
        if at:
            set_custom_root(at)

    cli()
