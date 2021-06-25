# Oveda Harzinfo

## Start container (idle)

```sh
docker run --name oveda-harzinfo danielgrams/oveda-harzinfo:latest
```

## Import (e.g. as cron job)

```sh
docker exec oveda-harzinfo python import.py
```

## Prepare Redis to re-import all

```sh
del event_hashes
del last_run
```
