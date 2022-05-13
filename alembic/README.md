# Database Migrations

Applying the "initial revision" on an already existing (up-to-date) db:

```shell
cd alembic/
DINO_ENVIRONMENT=someenv alembic stamp head
```

Run new migrations:

```shell
cd alembic/
DINO_ENVIRONMENT=someenv alembic upgrade head
```

Create a new revision file when models have changed:

```shell
cd alembic/
alembic revision -m "added column foo"
```

Then open the new `versions/xxx_added_column_foo.py` file and fill in the `upgrade()` and `downgrade()` methods.