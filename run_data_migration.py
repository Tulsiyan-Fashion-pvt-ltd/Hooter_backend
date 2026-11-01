import asyncio
import importlib
import pkgutil
import sql_data_migration


async def main():
    migration_modules = sorted(
        pkgutil.iter_modules(sql_data_migration.__path__),
        key=lambda module: module.name
    )

    for migration in migration_modules:
        if migration.name.startswith("_"):
            continue

        module = importlib.import_module(
            f"sql_data_migration.{migration.name}"
        )

        await module.run()


if __name__ == "__main__":
    asyncio.run(main())