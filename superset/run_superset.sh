# !/bin/bash
export SUPERSET_CONFIG_PATH=$PWD/superset/superset_config.py
# . /app/superset/superset_env/bin/activate
gunicorn \
      -w 10 \
      -k gevent \
      --timeout 120 \
      -b  0.0.0.0:8088 \
      --limit-request-line 0 \
      --limit-request-field_size 0 \
      "superset.app:create_app()"