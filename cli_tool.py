import click
from data import YahooFinanceAPI, SQLRepository


@click.group()
@click.pass_context
def cli(context):
    """Stock Data CLI Tool Entry Point."""
    context.ensure_object(dict)
    context.obj['db'] = SQLRepository('stock_data.db')
    context.obj['api'] = YahooFinanceAPI()


@cli.command()
@click.argument('ticker')
@click.argument('duration')
@click.pass_context
def get(context, ticker, duration):
    """Display the head of the stock data."""
    api = context.obj['api']
    data = api.get_stock_data(ticker, duration)
    click.echo(data)


@cli.command()
@click.argument('ticker')
@click.argument('duration')
@click.pass_context
def set(context, ticker, duration):
    """Store the stock data into SQLite."""
    api = context.obj['api']
    db = context.obj['db']
    data = api.get_stock_data(ticker, duration)
    result = db.insert_table(
        table_name=ticker, records=data, if_exists='replace')
    click.echo(f"Data inserted: {result['records_inserted']} rows.")


if __name__ == '__main__':
    cli()
