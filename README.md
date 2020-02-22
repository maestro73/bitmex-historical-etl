Deploy:
-------

1) Deploy as a Google Cloud Background Function, with a Pub/Sub trigger.
3) Create a Google Cloud Scheduler job to invoke the Pub/Sub trigger.

There is an `invoke` task:

```
invoke deploy
```

Every 10 minutes, the function will be invoked. There is no further processing if data is not available, or aggregated data already exists.

The `invoke` tasks also require `gcloud`. Also, state is stored in Firestore cache.

Script:
-------

There is a script, so you can get initial historical data. The following will load Google BigQuery with data from 2016-05-13 until now. 

```
python script.py
```

Requirements:
-------------

First, init a python venv:

```
python -m venv .env
```

Next, install requirements:

```
pip install -r requirements.txt

```

Extra requirements are in `requirements_extra`.

Environment:
------------

Rename `env.yaml.sample` to `env.yaml`, and add the required settings.
