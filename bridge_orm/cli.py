import argparse
import asyncio
from . import connect
from .introspect import reflect_table

async def main():
    parser = argparse.ArgumentParser(description="BridgeORM CLI")
    subparsers = parser.add_subparsers(dest="command")

    reflect_parser = subparsers.add_parser("reflect", help="Reflect a database table")
    reflect_parser.add_argument("--url", required=True, help="Database URL")
    reflect_parser.add_argument("--table", required=True, help="Table name to reflect")
    reflect_parser.add_argument("--output", help="Output file path")

    args = parser.parse_args()

    if args.command == "reflect":
        await connect(args.url)
        src = await reflect_table(args.table)
        if args.output:
            with open(args.output, "w") as f:
                f.write(src)
        else:
            print(src)

if __name__ == "__main__":
    asyncio.run(main())
