# Database Migrations

Applying the first revision ("initial revision") on an already existing db that wasn't using alembic before:

```shell
cd alembic/
DINO_ENVIRONMENT=someenv alembic stamp 81ca0935443a
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
